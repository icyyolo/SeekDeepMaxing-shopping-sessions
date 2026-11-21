//! Shopping Sessions: native pipeline.
//!
//! Reads  $DATA_DIR   (default /data)
//! Writes $OUTPUT_DIR (default /output): user_daily/ and campaign_daily/ as Parquet
//!
//! Stages (all parallel except the link graph):
//!   1. links  -> union-find -> id => person (byte-wise smallest id)
//!   2. events + clicks parsed from mmapped chunks; events bucketed by event_id hash,
//!      clicks by person hash
//!   3. per event_id bucket: dedup (smallest ingest_ts), keep valid, rebucket by person
//!   4. per person bucket: sort, sessionize, credit + assists, daily rows
//!   5. Parquet out

use hashbrown::HashTable;
use memchr::memchr;
use memmap2::Mmap;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const P: usize = 64; // buckets (power of two)
const CHUNK: usize = 4 << 20;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[inline]
fn hash(b: &[u8]) -> u64 {
    const K: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut h = (b.len() as u64).wrapping_mul(K);
    let mut c = b.chunks_exact(8);
    for w in &mut c {
        let v = u64::from_le_bytes(w.try_into().unwrap());
        h = (h ^ v).wrapping_mul(K).rotate_left(29);
    }
    let r = c.remainder();
    if !r.is_empty() {
        let mut t = [0u8; 8];
        t[..r.len()].copy_from_slice(r);
        h = (h ^ u64::from_le_bytes(t)).wrapping_mul(K).rotate_left(29);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

#[inline]
fn bucket(h: u64) -> usize {
    ((h >> 40) as usize) & (P - 1)
}

/// Split one line into exactly N fields (rules.md dialect). False if malformed.
/// Quotes must wrap a whole field; `""` is empty; no value holds a comma or quote.
#[inline]
fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let len = line.len();
    let mut i = 0;
    let mut n = 0;
    loop {
        let v: &'a [u8];
        if i < len && line[i] == b'"' {
            let mut j = i + 1;
            while j < len {
                let c = line[j];
                if c == b'"' {
                    break;
                }
                if c == b',' {
                    return false;
                }
                j += 1;
            }
            if j >= len {
                return false;
            }
            v = &line[i + 1..j];
            i = j + 1;
            if i < len && line[i] != b',' {
                return false;
            }
        } else {
            let mut j = i;
            while j < len {
                let c = line[j];
                if c == b',' {
                    break;
                }
                if c == b'"' {
                    return false;
                }
                j += 1;
            }
            v = &line[i..j];
            i = j;
        }
        if n == N {
            return false;
        }
        out[n] = v;
        n += 1;
        if i >= len {
            break;
        }
        i += 1; // the comma
    }
    n == N
}

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 {
        Some((a * 10 + c) as i64)
    } else {
        None
    }
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        2 => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// days_from_civil (Howard Hinnant)
fn civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Epoch ms, or None for a bad timestamp. Huge F3 values map to i64::MAX
/// (outside every window).
#[inline]
fn ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if b.iter().all(|c| c.is_ascii_digit()) {
        let mut k = 0;
        while k < n && b[k] == b'0' {
            k += 1;
        }
        if n - k > 15 {
            return Some(i64::MAX);
        }
        let mut v = 0i64;
        for &c in &b[k..] {
            v = v * 10 + (c - b'0') as i64;
        }
        return Some(v);
    }
    let zone_ok = (n == 20 && b[19] == b'Z')
        || (n == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':');
    if !zone_ok || b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let mut off = 0;
    if n == 25 {
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        off = (oh * 60 + om) * 60_000;
        if b[19] == b'-' {
            off = -off;
        }
    }
    Some((((civil(y, mo, d) * 24 + h) * 60 + mi) * 60 + s) * 1000 - off)
}

/// purchase price: digits only, 1..=10_000_000; 0 if invalid
#[inline]
fn price(b: &[u8]) -> u32 {
    if b.is_empty() || !b.iter().all(|c| c.is_ascii_digit()) {
        return 0;
    }
    let mut k = 0;
    while k < b.len() && b[k] == b'0' {
        k += 1;
    }
    if b.len() - k > 8 {
        return 0;
    }
    let mut v = 0u32;
    for &c in &b[k..] {
        v = v * 10 + (c - b'0') as u32;
    }
    if v > 10_000_000 {
        0
    } else {
        v
    }
}

fn par_map<R: Send, F: Fn(usize) -> R + Sync>(n: usize, threads: usize, f: F) -> Vec<R> {
    let next = AtomicUsize::new(0);
    let f = &f;
    let next = &next;
    let parts: Vec<Vec<(usize, R)>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads.min(n).max(1))
            .map(|_| {
                s.spawn(move || {
                    let mut v = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        v.push((i, f(i)));
                    }
                    v
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut all: Vec<(usize, R)> = parts.into_iter().flatten().collect();
    all.sort_unstable_by_key(|x| x.0);
    all.into_iter().map(|x| x.1).collect()
}

/// Call f on every data line (header skipped) of buf[start..end].
#[inline]
fn for_lines<'a>(buf: &'a [u8], start: usize, end: usize, mut f: impl FnMut(&'a [u8])) {
    let mut pos = start;
    while pos < end {
        match memchr(b'\n', &buf[pos..end]) {
            Some(k) => {
                f(&buf[pos..pos + k]);
                pos += k + 1;
            }
            None => {
                f(&buf[pos..end]);
                pos = end;
            }
        }
    }
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn map_file(p: &Path) -> Option<Mmap> {
    let f = File::open(p).ok()?;
    if f.metadata().ok()?.len() == 0 {
        return None;
    }
    unsafe { memmap2::MmapOptions::new().populate().map(&f).ok() }
}

/// (file index, start, end) chunks over data lines (header skipped), split after '\n'.
fn chunks(bufs: &[&[u8]], range: std::ops::Range<usize>, out: &mut Vec<(usize, usize, usize)>) {
    for k in range {
        let b = bufs[k];
        let Some(i) = memchr(b'\n', b) else { continue }; // header only
        let mut s = i + 1;
        while s < b.len() {
            let mut e = (s + CHUNK).min(b.len());
            if e < b.len() {
                e = memchr(b'\n', &b[e..]).map_or(b.len(), |i| e + i + 1);
            }
            out.push((k, s, e));
            s = e;
        }
    }
}

/// Lookup entry: id (inline when <= 16 bytes) -> person and its hash.
#[derive(Clone, Copy)]
struct LEnt<'a> {
    h: u64,
    ph: u64,
    person: &'a [u8],
    id: &'a [u8],
    key: [u8; 16],
}

#[inline]
fn key16(x: &[u8]) -> [u8; 16] {
    let mut k = [0u8; 16];
    let n = x.len().min(16);
    k[..n].copy_from_slice(&x[..n]);
    k
}

/// Linked IDs: id -> person (byte-wise smallest id of its component).
struct Links<'a> {
    look: HashTable<LEnt<'a>>,
    n: usize,
}

impl<'a> Links<'a> {
    fn build(edges: &[Vec<(&'a [u8], &'a [u8])>]) -> Self {
        fn find(p: &mut [u32], mut x: u32) -> u32 {
            while p[x as usize] != x {
                let g = p[p[x as usize] as usize];
                p[x as usize] = g;
                x = g;
            }
            x
        }
        let mut ids: Vec<&'a [u8]> = Vec::new();
        let mut idt: HashTable<u32> = HashTable::new();
        let mut parent: Vec<u32> = Vec::new();
        for v in edges {
            for &(a, b) in v {
                let mut ix = [0u32; 2];
                for (slot, x) in ix.iter_mut().zip([a, b]) {
                    let h = hash(x);
                    *slot = match idt.find(h, |&i| ids[i as usize] == x) {
                        Some(&i) => i,
                        None => {
                            let i = ids.len() as u32;
                            ids.push(x);
                            parent.push(i);
                            idt.insert_unique(h, i, |&j| hash(ids[j as usize]));
                            i
                        }
                    };
                }
                let (ra, rb) = (find(&mut parent, ix[0]), find(&mut parent, ix[1]));
                if ra != rb {
                    parent[ra.max(rb) as usize] = ra.min(rb);
                }
            }
        }
        let n = ids.len();
        let mut best: Vec<u32> = (0..n as u32).collect();
        let mut root = vec![0u32; n];
        for i in 0..n {
            let r = find(&mut parent, i as u32);
            root[i] = r;
            if ids[i] < ids[best[r as usize] as usize] {
                best[r as usize] = i as u32;
            }
        }
        drop(idt);
        let mut look: HashTable<LEnt<'a>> = HashTable::with_capacity(n);
        let mut ph_of_root: Vec<u64> = vec![0; n];
        for i in 0..n {
            let r = root[i] as usize;
            if best[r] as usize == i || ph_of_root[r] == 0 {
                ph_of_root[r] = hash(ids[best[r] as usize]);
            }
        }
        for i in 0..n {
            let r = root[i] as usize;
            let id = ids[i];
            let h = hash(id);
            let e = LEnt { h, ph: ph_of_root[r], person: ids[best[r] as usize], id, key: key16(id) };
            look.insert_unique(h, e, |x| x.h);
        }
        Links { look, n }
    }

    /// (person, hash(person))
    #[inline]
    fn resolve(&self, uid: &'a [u8]) -> (&'a [u8], u64) {
        let h = hash(uid);
        let k = key16(uid);
        let long = uid.len() > 16;
        match self.look.find(h, |e| e.h == h && e.id.len() == uid.len() && e.key == k && (!long || e.id == uid)) {
            Some(e) => (e.person, e.ph),
            None => (uid, h),
        }
    }
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct EvA<'a> {
    h: u64,
    eid: &'a [u8],
    uid: &'a [u8],
    ing: i64,
    ts: i64,
    price: u32, // > 0 only for a valid purchase
    cat: u16,
    ok: bool,
}

#[derive(Clone, Copy)]
struct EvB<'a> {
    ph: u64,
    person: &'a [u8],
    ts: i64,
    price: u32,
    cat: u16,
}

#[derive(Clone, Copy)]
struct Clk<'a> {
    ph: u64,
    cid: &'a [u8],
    ts: i64,
    camp: u16,
}

struct Ud<'a> {
    person: &'a [u8],
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u16>,
}

// ---------------------------------------------------------------------------
// Per-person work
// ---------------------------------------------------------------------------

struct Ctx<'a> {
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u16>,
    cat: Vec<i64>,
    touched: Vec<u16>,
    agg: Vec<[i64; 4]>, // (campaign incl. organic) x day: sessions, purchasing, revenue, assisted
    ud: Vec<Ud<'a>>,
    ncamp: usize,
    ndays: usize,
    day0: i64,
}

impl<'a> Ctx<'a> {
    fn new(ncamp: usize, ncat: usize, ndays: usize, day0: i64) -> Self {
        Ctx {
            cnt: vec![0; ncamp],
            pos: vec![0; ncamp],
            active: Vec::new(),
            cat: vec![0; ncat],
            touched: Vec::new(),
            agg: vec![[0; 4]; (ncamp + 1) * ndays],
            ud: Vec::new(),
            ncamp,
            ndays,
            day0,
        }
    }

    #[inline]
    fn add(&mut self, c: u16) {
        let ci = c as usize;
        if self.cnt[ci] == 0 {
            self.pos[ci] = self.active.len() as u32;
            self.active.push(c);
        }
        self.cnt[ci] += 1;
    }

    #[inline]
    fn rem(&mut self, c: u16) {
        let ci = c as usize;
        self.cnt[ci] -= 1;
        if self.cnt[ci] == 0 {
            let p = self.pos[ci] as usize;
            let last = *self.active.last().unwrap();
            self.active.swap_remove(p);
            if last != c {
                self.pos[last as usize] = p as u32;
            }
        }
    }

    fn flush_day(&mut self, row: Ud<'a>) {
        let mut top: Option<u16> = None;
        let mut best = 0i64;
        for &c in &self.touched {
            let v = self.cat[c as usize];
            if top.is_none() || v > best || (v == best && c < top.unwrap()) {
                top = Some(c);
                best = v;
            }
            self.cat[c as usize] = 0;
        }
        self.touched.clear();
        self.ud.push(Ud { top, ..row });
    }

    /// ev sorted by ts; ck sorted by (ts, click_id)
    fn person(&mut self, person: &'a [u8], ev: &[EvB<'a>], ck: &[Clk<'a>]) {
        let (mut lo, mut hi) = (0usize, 0usize);
        let mut row: Option<Ud<'a>> = None;
        let mut k = 0;
        while k < ev.len() {
            let start = ev[k].ts;
            let day = start.div_euclid(DAY);
            if row.as_ref().map_or(true, |r| r.day as i64 != day) {
                if let Some(r) = row.take() {
                    self.flush_day(r);
                }
                row = Some(Ud { person, day: day as i32, sessions: 0, events: 0, revenue: 0, top: None });
            }
            let mut n = 0i64;
            let mut rev = 0i64;
            let mut prev = start;
            while k < ev.len() && ev[k].ts - prev <= GAP {
                let e = &ev[k];
                n += 1;
                if e.price > 0 {
                    rev += e.price as i64;
                    let ci = e.cat as usize;
                    if self.cat[ci] == 0 {
                        self.touched.push(e.cat);
                    }
                    self.cat[ci] += e.price as i64;
                }
                prev = e.ts;
                k += 1;
            }
            let r = row.as_mut().unwrap();
            r.sessions += 1;
            r.events += n;
            r.revenue += rev;

            while hi < ck.len() && ck[hi].ts <= start {
                self.add(ck[hi].camp);
                hi += 1;
            }
            while lo < hi && ck[lo].ts < start - WEEK {
                self.rem(ck[lo].camp);
                lo += 1;
            }
            let camp = if hi > lo {
                let t = ck[hi - 1].ts;
                let mut j = hi - 1;
                while j > lo && ck[j - 1].ts == t {
                    j -= 1;
                }
                ck[j].camp as usize
            } else {
                self.ncamp
            };
            let di = (day - self.day0) as usize;
            let nd = self.ndays;
            let a = &mut self.agg[camp * nd + di];
            a[0] += 1;
            if rev > 0 {
                a[1] += 1;
            }
            a[2] += rev;
            for &c in &self.active {
                self.agg[c as usize * nd + di][3] += 1;
            }
        }
        if let Some(r) = row.take() {
            self.flush_day(r);
        }
        for &c in &self.active {
            self.cnt[c as usize] = 0;
        }
        self.active.clear();
    }
}

// ---------------------------------------------------------------------------
// Parquet
// ---------------------------------------------------------------------------

fn write_parquet(path: &Path, schema: &str, write: impl FnOnce(&mut Vec<ColData>)) {
    let schema = Arc::new(parse_message_type(schema).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let mut cols: Vec<ColData> = Vec::new();
    write(&mut cols);
    let file = File::create(path).unwrap();
    let mut w = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut rg = w.next_row_group().unwrap();
    let mut it = cols.into_iter();
    while let Some(mut col) = rg.next_column().unwrap() {
        match it.next().unwrap() {
            ColData::Str(v, def) => {
                col.typed::<ByteArrayType>().write_batch(&v, def.as_deref(), None).unwrap();
            }
            ColData::I32(v) => {
                col.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }
            ColData::I64(v) => {
                col.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            }
        }
        col.close().unwrap();
    }
    rg.close().unwrap();
    w.close().unwrap();
}

enum ColData {
    Str(Vec<ByteArray>, Option<Vec<i16>>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

const UD_SCHEMA: &str = "message user_daily {
  REQUIRED BYTE_ARRAY user_id (UTF8);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 events;
  REQUIRED INT64 revenue_cents;
  OPTIONAL BYTE_ARRAY top_category (UTF8);
}";

const CD_SCHEMA: &str = "message campaign_daily {
  REQUIRED BYTE_ARRAY campaign_id (UTF8);
  REQUIRED BYTE_ARRAY channel (UTF8);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 purchasing_sessions;
  REQUIRED INT64 revenue_cents;
  REQUIRED INT64 assisted_sessions;
}";

fn write_ud(path: &Path, rows: &[Ud], cats: &[Vec<u8>]) {
    write_parquet(path, UD_SCHEMA, |cols| {
        cols.push(ColData::Str(rows.iter().map(|r| ByteArray::from(r.person.to_vec())).collect(), None));
        cols.push(ColData::I32(rows.iter().map(|r| r.day).collect()));
        cols.push(ColData::I64(rows.iter().map(|r| r.sessions).collect()));
        cols.push(ColData::I64(rows.iter().map(|r| r.events).collect()));
        cols.push(ColData::I64(rows.iter().map(|r| r.revenue).collect()));
        let def: Vec<i16> = rows.iter().map(|r| r.top.is_some() as i16).collect();
        let vals: Vec<ByteArray> =
            rows.iter().filter_map(|r| r.top.map(|c| ByteArray::from(cats[c as usize].clone()))).collect();
        cols.push(ColData::Str(vals, Some(def)));
    });
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn small_csv<const N: usize>(path: &Path) -> Vec<[Vec<u8>; N]> {
    let buf = fs::read(path).unwrap_or_default();
    let start = memchr(b'\n', &buf).map_or(buf.len(), |i| i + 1);
    let mut out = Vec::new();
    for_lines(&buf, start, buf.len(), |line| {
        let mut f: [&[u8]; N] = [&[]; N];
        if split(line, &mut f) {
            out.push(f.map(|x| x.to_vec()));
        }
    });
    out
}

fn json_str(s: &str, key: &str) -> String {
    let i = s.find(&format!("\"{key}\"")).expect("manifest key");
    let rest = &s[i + key.len() + 2..];
    let a = rest.find('"').unwrap() + 1;
    let b = a + rest[a..].find('"').unwrap();
    rest[a..b].to_string()
}

fn main() {
    let t0 = Instant::now();
    let log = |m: &str| eprintln!("[{:7.3}s] {m}", t0.elapsed().as_secs_f64());
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()));

    let manifest = fs::read_to_string(data.join("manifest.json")).unwrap();
    let ws = ts(json_str(&manifest, "window_start").as_bytes()).unwrap();
    let we = ts(json_str(&manifest, "window_end").as_bytes()).unwrap();
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;

    // products -> category index (categories sorted byte-wise, so a tie picks the lower index)
    let products = small_csv::<2>(&data.join("products.csv"));
    let mut cats: Vec<Vec<u8>> = products.iter().map(|r| r[1].clone()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    let unknown = cats.binary_search(&b"unknown".to_vec()).unwrap() as u16;
    let mut prod: HashTable<(&[u8], u16)> = HashTable::with_capacity(products.len());
    for r in &products {
        let c = cats.binary_search(&r[1]).unwrap() as u16;
        let h = hash(&r[0]);
        if prod.find(h, |x| x.0 == &r[0][..]).is_none() {
            prod.insert_unique(h, (&r[0][..], c), |x| hash(x.0));
        }
    }
    let campaigns = small_csv::<2>(&data.join("campaigns.csv"));
    let ncamp = campaigns.len();
    let mut camp: HashTable<(&[u8], u16)> = HashTable::with_capacity(ncamp);
    for (i, r) in campaigns.iter().enumerate() {
        camp.insert_unique(hash(&r[0]), (&r[0][..], i as u16), |x| hash(x.0));
    }

    // mmap everything up front
    let ev_files = csv_files(&data.join("events"));
    let ck_files = csv_files(&data.join("clicks"));
    let ln_files = csv_files(&data.join("links"));
    let all_files: Vec<&PathBuf> = ev_files.iter().chain(&ck_files).chain(&ln_files).collect();
    let maps: Vec<Option<Mmap>> = par_map(all_files.len(), threads, |i| map_file(all_files[i]));
    let empty: &[u8] = &[];
    let bufs: Vec<&[u8]> = maps.iter().map(|m| m.as_deref().unwrap_or(empty)).collect();
    let (ne, nc) = (ev_files.len(), ck_files.len());
    let mut ev_chunks = Vec::new();
    let mut ck_chunks = Vec::new();
    let mut ln_chunks = Vec::new();
    chunks(&bufs, 0..ne, &mut ev_chunks);
    chunks(&bufs, ne..ne + nc, &mut ck_chunks);
    chunks(&bufs, ne + nc..bufs.len(), &mut ln_chunks);
    log(&format!("mapped {} event, {} click, {} link files", ne, nc, ln_files.len()));

    // ---- links (one thread) overlapped with event parsing (all threads) ----
    let parse_events = |i: usize| -> Vec<Vec<EvA>> {
        let (k, s, e) = ev_chunks[i];
        let cap = (e - s) / 70 / P + 16;
        let mut out: Vec<Vec<EvA>> = (0..P).map(|_| Vec::with_capacity(cap)).collect();
        for_lines(bufs[k], s, e, |line| {
            let mut f: [&[u8]; 7] = [&[]; 7];
            if !split(line, &mut f) || f[0].is_empty() {
                return;
            }
            let ing = ts(f[3]).unwrap_or(i64::MAX);
            let et = f[4];
            let mut ok = !f[1].is_empty() && !f[5].is_empty();
            let mut p = 0u32;
            let mut cat = 0u16;
            let t = match ts(f[2]) {
                Some(t) if t >= ws && t < we => t,
                _ => {
                    ok = false;
                    0
                }
            };
            if et == b"purchase" {
                p = price(f[6]);
                if p == 0 {
                    ok = false;
                } else {
                    cat = prod.find(hash(f[5]), |x| x.0 == f[5]).map_or(unknown, |x| x.1);
                }
            } else if et != b"view" && et != b"cart" {
                ok = false;
            }
            if ok && ing.saturating_sub(t) > DAY {
                ok = false;
            }
            let h = hash(f[0]);
            out[bucket(h)].push(EvA { h, eid: f[0], uid: f[1], ing, ts: t, price: p, cat, ok });
        });
        out
    };
    let (links, evp) = std::thread::scope(|s| {
        let lh = s.spawn(|| {
            let mut edges = Vec::new();
            for &(k, s, e) in &ln_chunks {
                for_lines(bufs[k], s, e, |line| {
                    let mut f: [&[u8]; 2] = [&[]; 2];
                    if split(line, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
                        edges.push((f[0], f[1]));
                    }
                });
            }
            Links::build(&[edges])
        });
        let evp: Vec<Vec<Vec<EvA>>> = par_map(ev_chunks.len(), threads, parse_events);
        (lh.join().unwrap(), evp)
    });
    log(&format!("parsed events; links: {} ids", links.n));

    // ---- clicks ----
    let ckp: Vec<Vec<Vec<Clk>>> = par_map(ck_chunks.len(), threads, |i| {
        let (k, s, e) = ck_chunks[i];
        let cap = (e - s) / 50 / P + 16;
        let mut out: Vec<Vec<Clk>> = (0..P).map(|_| Vec::with_capacity(cap)).collect();
        for_lines(bufs[k], s, e, |line| {
            let mut f: [&[u8]; 4] = [&[]; 4];
            if !split(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                return;
            }
            let t = match ts(f[3]) {
                Some(t) if t >= ws - WEEK && t < we => t,
                _ => return,
            };
            let Some(&(_, c)) = camp.find(hash(f[2]), |x| x.0 == f[2]) else { return };
            let (_, ph) = links.resolve(f[1]);
            out[bucket(ph)].push(Clk { ph, cid: f[0], ts: t, camp: c });
        });
        out
    });
    log("parsed clicks");

    // ---- dedup events (smallest ingest_ts wins), rebucket survivors by person ----
    let byperson: Vec<Vec<Vec<EvB>>> = par_map(P, threads, |p| {
        let parts: Vec<&[EvA]> = evp.iter().map(|b| &b[p][..]).collect();
        let mut offs = Vec::with_capacity(parts.len() + 1);
        offs.push(0usize);
        for q in &parts {
            offs.push(offs.last().unwrap() + q.len());
        }
        let total = *offs.last().unwrap();
        let get = |i: u32| -> &EvA {
            let i = i as usize;
            let q = offs.partition_point(|&o| o <= i) - 1;
            &parts[q][i - offs[q]]
        };
        let mut t: HashTable<u32> = HashTable::with_capacity(total);
        let mut lose = vec![false; total];
        let mut gi = 0u32;
        for q in &parts {
            for e in q.iter() {
                match t.find_mut(e.h, |&j| {
                    let x = get(j);
                    x.h == e.h && x.eid == e.eid
                }) {
                    Some(j) => {
                        if e.ing < get(*j).ing {
                            lose[*j as usize] = true;
                            *j = gi;
                        } else {
                            lose[gi as usize] = true;
                        }
                    }
                    None => {
                        t.insert_unique(e.h, gi, |&j| get(j).h);
                    }
                }
                gi += 1;
            }
        }
        drop(t);
        let mut out: Vec<Vec<EvB>> = (0..P).map(|_| Vec::with_capacity(total / P + 16)).collect();
        let mut gi = 0usize;
        for q in &parts {
            for e in q.iter() {
                if e.ok && !lose[gi] {
                    let (person, ph) = links.resolve(e.uid);
                    out[bucket(ph)].push(EvB { ph, person, ts: e.ts, price: e.price, cat: e.cat });
                }
                gi += 1;
            }
        }
        out
    });
    log("dedup done");

    // ---- per person (a person is identified by its 64-bit name hash) ----
    let ncat = cats.len();
    let ctxs: Vec<Ctx> = par_map(P, threads, |p| {
        let mut ev: Vec<EvB> = Vec::with_capacity(byperson.iter().map(|b| b[p].len()).sum());
        for b in &byperson {
            ev.extend_from_slice(&b[p]);
        }
        ev.sort_unstable_by_key(|e| (e.ph, e.ts));
        let mut ck: Vec<Clk> = Vec::with_capacity(ckp.iter().map(|b| b[p].len()).sum());
        for b in &ckp {
            ck.extend_from_slice(&b[p]);
        }
        ck.sort_unstable_by(|a, b| (a.ph, a.ts).cmp(&(b.ph, b.ts)).then_with(|| a.cid.cmp(b.cid)));
        let mut ctx = Ctx::new(ncamp, ncat, ndays, day0);
        let (mut i, mut j) = (0usize, 0usize);
        while i < ev.len() {
            let ph = ev[i].ph;
            let mut ie = i + 1;
            while ie < ev.len() && ev[ie].ph == ph {
                ie += 1;
            }
            while j < ck.len() && ck[j].ph < ph {
                j += 1;
            }
            let mut je = j;
            while je < ck.len() && ck[je].ph == ph {
                je += 1;
            }
            ctx.person(ev[i].person, &ev[i..ie], &ck[j..je]);
            i = ie;
            j = je;
        }
        ctx
    });
    log("sessions done");

    // ---- outputs ----
    let ud_dir = out.join("user_daily");
    let cd_dir = out.join("campaign_daily");
    fs::create_dir_all(&ud_dir).unwrap();
    fs::create_dir_all(&cd_dir).unwrap();
    let nonempty: Vec<usize> = (0..P).filter(|&p| !ctxs[p].ud.is_empty()).collect();
    if nonempty.is_empty() {
        write_ud(&ud_dir.join("part-0.parquet"), &[], &cats);
    }
    par_map(nonempty.len(), threads, |i| {
        let p = nonempty[i];
        write_ud(&ud_dir.join(format!("part-{p}.parquet")), &ctxs[p].ud, &cats);
    });

    let mut agg = vec![[0i64; 4]; (ncamp + 1) * ndays];
    for c in &ctxs {
        for (a, b) in agg.iter_mut().zip(&c.agg) {
            for q in 0..4 {
                a[q] += b[q];
            }
        }
    }
    let mut cid = Vec::new();
    let mut chan = Vec::new();
    let mut day = Vec::new();
    let mut cols: [Vec<i64>; 4] = Default::default();
    for c in 0..=ncamp {
        for d in 0..ndays {
            let a = agg[c * ndays + d];
            if a[0] == 0 && a[3] == 0 {
                continue;
            }
            if c == ncamp {
                cid.push(ByteArray::from(b"organic".to_vec()));
                chan.push(ByteArray::from(b"organic".to_vec()));
            } else {
                cid.push(ByteArray::from(campaigns[c][0].clone()));
                chan.push(ByteArray::from(campaigns[c][1].clone()));
            }
            day.push((day0 + d as i64) as i32);
            for q in 0..4 {
                cols[q].push(a[q]);
            }
        }
    }
    write_parquet(&cd_dir.join("part-0.parquet"), CD_SCHEMA, |out| {
        out.push(ColData::Str(cid, None));
        out.push(ColData::Str(chan, None));
        out.push(ColData::I32(day));
        let [a, b, c, d] = cols;
        out.push(ColData::I64(a));
        out.push(ColData::I64(b));
        out.push(ColData::I64(c));
        out.push(ColData::I64(d));
    });
    log("done");
}
