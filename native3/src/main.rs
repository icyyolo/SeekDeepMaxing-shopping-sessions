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

// ---------------------------------------------------------------------------
// Allocator: per-thread bump slabs inside one huge-page (THP madvise) reservation.
// Frees are no-ops (except undoing the thread's latest allocation); the process
// exits right after writing its output. Fresh 4K-page faults and TLB misses were
// the main cost under virtualization.
// ---------------------------------------------------------------------------
mod bump {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const RESERVE: usize = 64 << 30;
    const SLAB: usize = 32 << 20;
    const HUGE: usize = 2 << 20;
    static BASE: AtomicUsize = AtomicUsize::new(0);
    static TOP: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        // (next free, slab end, start of latest allocation)
        static CUR: Cell<(usize, usize, usize)> = const { Cell::new((0, 0, 0)) };
    }
    extern "C" {
        fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
        fn munmap(addr: *mut u8, len: usize) -> i32;
        fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    }

    fn base() -> usize {
        let b = BASE.load(Ordering::Acquire);
        if b != 0 {
            return b;
        }
        // PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE
        let p = unsafe { mmap(std::ptr::null_mut(), RESERVE + HUGE, 3, 0x02 | 0x20 | 0x4000, -1, 0) } as usize;
        if p == usize::MAX || p == 0 {
            return 0;
        }
        let a = (p + HUGE - 1) & !(HUGE - 1);
        unsafe { madvise(a as *mut u8, RESERVE, 14) }; // MADV_HUGEPAGE
        match BASE.compare_exchange(0, a, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => a,
            Err(x) => {
                unsafe { munmap(p as *mut u8, RESERVE + HUGE) };
                x
            }
        }
    }

    fn grab(n: usize) -> usize {
        let b = base();
        if b == 0 {
            return 0;
        }
        let n = (n + HUGE - 1) & !(HUGE - 1);
        let off = TOP.fetch_add(n, Ordering::Relaxed);
        if off + n > RESERVE {
            return 0;
        }
        b + off
    }

    #[inline]
    fn mine(x: usize) -> bool {
        let b = BASE.load(Ordering::Relaxed);
        b != 0 && x >= b && x < b + RESERVE
    }

    pub fn used_mb() -> usize {
        TOP.load(Ordering::Relaxed) >> 20
    }

    pub struct Bump;

    unsafe impl GlobalAlloc for Bump {
        #[inline]
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            let (size, align) = (l.size(), l.align());
            if size >= SLAB / 4 {
                let p = grab(size);
                return if p == 0 { System.alloc(l) } else { p as *mut u8 };
            }
            CUR.with(|c| {
                let (mut p, mut end, _) = c.get();
                let mut a = (p + align - 1) & !(align - 1);
                if p == 0 || a + size > end {
                    let s = grab(SLAB);
                    if s == 0 {
                        return System.alloc(l);
                    }
                    p = s;
                    end = s + SLAB;
                    a = (p + align - 1) & !(align - 1);
                }
                c.set((a + size, end, a));
                a as *mut u8
            })
        }

        #[inline]
        unsafe fn dealloc(&self, ptr: *mut u8, l: Layout) {
            let x = ptr as usize;
            if mine(x) {
                CUR.with(|c| {
                    let (p, end, last) = c.get();
                    if last == x && p == x + l.size() {
                        c.set((x, end, 0));
                    }
                });
            } else {
                System.dealloc(ptr, l)
            }
        }

        #[inline]
        unsafe fn realloc(&self, ptr: *mut u8, l: Layout, new: usize) -> *mut u8 {
            let x = ptr as usize;
            if mine(x) {
                let grown = CUR.with(|c| {
                    let (p, end, last) = c.get();
                    if last == x && p == x + l.size() && x + new <= end {
                        c.set((x + new, end, x));
                        true
                    } else {
                        false
                    }
                });
                if grown {
                    return ptr;
                }
            }
            let nl = Layout::from_size_align_unchecked(new, l.align());
            let np = self.alloc(nl);
            if !np.is_null() {
                std::ptr::copy_nonoverlapping(ptr, np, l.size().min(new));
                self.dealloc(ptr, l);
            }
            np
        }
    }
}

#[global_allocator]
static ALLOC: bump::Bump = bump::Bump;

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

/// A byte slice packed into 8 bytes: pointer (48 bits) | length (16 bits).
/// Every slice points into an mmapped input that outlives all records.
#[derive(Clone, Copy)]
struct S(u64);

impl S {
    #[inline]
    fn new(b: &[u8]) -> S {
        debug_assert!(b.len() < 1 << 16);
        S((b.as_ptr() as u64) | ((b.len() as u64) << 48))
    }
    #[inline]
    fn get<'a>(self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts((self.0 & ((1 << 48) - 1)) as *const u8, (self.0 >> 48) as usize) }
    }
}

#[inline]
fn bucket(h: u64) -> usize {
    ((h >> 40) as usize) & (P - 1)
}

const PD: usize = 2048; // dedup buckets (power of two)

#[inline]
fn dbucket(h: u64) -> usize {
    ((h >> 20) as usize) & (PD - 1)
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
/// (outside every window). Same results as ts_ref, fewer instructions.
#[inline]
fn ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 20 && b[10] == b'T' {
        return iso_z(b);
    }
    if (n == 20 || n == 25) && b[10] == b'T' {
        return iso(b);
    }
    if n == 13 {
        let x = u64::from_le_bytes(b[0..8].try_into().unwrap());
        let y = u64::from_le_bytes(b[5..13].try_into().unwrap());
        if !digits_ok(x, !0) || !digits_ok(y, !0) {
            return None;
        }
        return Some((parse8(x) * 100_000 + parse8(y) % 100_000) as i64);
    }
    if n == 0 {
        return None;
    }
    let mut v: i64 = 0;
    let mut sig = 0usize;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if sig > 0 || d != 0 {
            sig += 1;
            if sig <= 15 {
                v = v * 10 + d as i64;
            }
        }
    }
    Some(if sig > 15 { i64::MAX } else { v })
}

/// True if every byte selected by m (0xFF per byte) is an ASCII digit.
#[inline(always)]
fn digits_ok(x: u64, m: u64) -> bool {
    let d = (x ^ 0x3030_3030_3030_3030) & m;
    (d | d.wrapping_add(0x7676_7676_7676_7676 & m)) & 0x8080_8080_8080_8080 & m == 0
}

/// Eight ASCII digits (first char in the low byte) to their value.
#[inline(always)]
fn parse8(x: u64) -> u64 {
    let v = x.wrapping_sub(0x3030_3030_3030_3030);
    let v = (v.wrapping_mul(10)).wrapping_add(v >> 8) & 0x00FF_00FF_00FF_00FF;
    let v = (v.wrapping_mul(100)).wrapping_add(v >> 16) & 0x0000_FFFF_0000_FFFF;
    (v.wrapping_mul(10000)).wrapping_add(v >> 32) & 0xFFFF_FFFF
}

/// ts() for 20-byte input: the "...Z" ISO form, None for anything else.
#[inline(always)]
fn iso_z(b: &[u8]) -> Option<i64> {
    const M1: u64 = 0x00FF_FF00_FFFF_FFFF; // "YYYY-MM-" digit bytes
    const M2: u64 = 0xFFFF_00FF_FF00_FFFF; // "DDTHH:MM"
    const M3: u64 = 0x00FF_FF00; // ":SSZ"
    const Z: u64 = 0x3030_3030_3030_3030;
    let x1 = u64::from_le_bytes(b[0..8].try_into().unwrap());
    let x2 = u64::from_le_bytes(b[8..16].try_into().unwrap());
    let x3 = u32::from_le_bytes(b[16..20].try_into().unwrap()) as u64;
    if x1 & !M1 != 0x2D00_002D_0000_0000
        || x2 & !M2 != 0x0000_3A00_0054_0000
        || x3 & !M3 != 0x5A00_003A
        || !digits_ok(x1, M1)
        || !digits_ok(x2, M2)
        || !digits_ok(x3, M3)
    {
        return None;
    }
    // digit values (separators zeroed); byte k of w = 10 * digit k + digit k+1
    let (v1, v2, v3) = ((x1 ^ Z) & M1, (x2 ^ Z) & M2, (x3 ^ Z) & M3);
    let (w1, w2, w3) = (v1 * 10 + (v1 >> 8), v2 * 10 + (v2 >> 8), v3 * 10 + (v3 >> 8));
    let y = (w1 & 0xFF) * 100 + ((w1 >> 16) & 0xFF);
    let mo = (w1 >> 40) & 0xFF;
    let d = w2 & 0xFF;
    let h = (w2 >> 24) & 0xFF;
    let mi = (w2 >> 48) & 0xFF;
    let sec = (w3 >> 8) & 0xFF;
    if mo.wrapping_sub(1) >= 12 || d == 0 || h > 23 || mi > 59 || sec > 59 {
        return None;
    }
    if d > 28 {
        const DIM: [u64; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        if d > DIM[mo as usize] + (mo == 2 && leap) as u64 {
            return None;
        }
    }
    // days_from_civil, shifted 400 years so everything stays unsigned
    let yy = y + 400 - (mo <= 2) as u64;
    let mp = if mo > 2 { mo - 3 } else { mo + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let days = (yy * 365 + yy / 4 - yy / 100 + yy / 400 + doy) as i64 - 719_468 - 146_097;
    Some(((days * 24 + h as i64) * 60 + mi as i64) * 60_000 + sec as i64 * 1000)
}

#[inline]
fn iso(b: &[u8]) -> Option<i64> {
    {
        // "YYYY-MM-" "DDTHH:MM" ":SS?" : digits and separators in three loads
        let x1 = u64::from_le_bytes(b[0..8].try_into().unwrap());
        let x2 = u64::from_le_bytes(b[8..16].try_into().unwrap());
        let x3 = u32::from_le_bytes(b[16..20].try_into().unwrap()) as u64;
        if x1 & 0xFF00_00FF_0000_0000 != 0x2D00_002D_0000_0000
            || x2 & 0x0000_FF00_00FF_0000 != 0x0000_3A00_0054_0000
            || x3 & 0xFF != 0x3A
            || !digits_ok(x1, 0x00FF_FF00_FFFF_FFFF)
            || !digits_ok(x2, 0xFFFF_00FF_FF00_FFFF)
            || !digits_ok(x3, 0x00FF_FF00)
        {
            return None;
        }
    }
    let n = b.len();
    let zone_ok = (n == 20 && b[19] == b'Z') || (n == 25 && (b[19] == b'+' || b[19] == b'-') && b[22] == b':');
    if !zone_ok {
        return None;
    }
    let g = |i: usize| b[i].wrapping_sub(b'0') as i64;
    let bad = |i: usize| b[i].wrapping_sub(b'0') > 9;
    let y = g(0) * 1000 + g(1) * 100 + g(2) * 10 + g(3);
    let mo = g(5) * 10 + g(6);
    let d = g(8) * 10 + g(9);
    let h = g(11) * 10 + g(12);
    let mi = g(14) * 10 + g(15);
    let s = g(17) * 10 + g(18);
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    const DIM: [i64; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    const CUM: [i64; 13] = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let dim = DIM[mo as usize] + (mo == 2 && leap) as i64;
    if d > dim {
        return None;
    }
    let mut off = 0;
    if n == 25 {
        if bad(20) | bad(21) | bad(23) | bad(24) {
            return None;
        }
        let oh = g(20) * 10 + g(21);
        let om = g(23) * 10 + g(24);
        if oh > 23 || om > 59 {
            return None;
        }
        off = (oh * 60 + om) * 60_000;
        if b[19] == b'-' {
            off = -off;
        }
    }
    // days from 0001-01-01 to y-01-01, shifted 400 years so the divisions stay non-negative
    let a = y + 399;
    let days = a * 365 + a / 4 - a / 100 + a / 400 - 146_097 - 719_162
        + CUM[mo as usize]
        + (mo > 2 && leap) as i64
        + d
        - 1;
    Some(((days * 24 + h) * 60 + mi) * 60_000 + s * 1000 - off)
}

#[allow(dead_code)]
fn ts_ref(b: &[u8]) -> Option<i64> {
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

fn par_map_init<T, R: Send, I: Fn() -> T + Sync, F: Fn(&mut T, usize) -> R + Sync>(
    n: usize,
    threads: usize,
    init: I,
    f: F,
) -> Vec<R> {
    let next = AtomicUsize::new(0);
    let (f, init, next) = (&f, &init, &next);
    let parts: Vec<Vec<(usize, R)>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads.min(n).max(1))
            .map(|_| {
                s.spawn(move || {
                    let mut st = init();
                    let mut v = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        v.push((i, f(&mut st, i)));
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

/// Like par_map_init, but returns each worker's final state instead of per-item results.
fn par_states<T: Send, I: Fn() -> T + Sync, F: Fn(&mut T, usize) + Sync>(n: usize, threads: usize, init: I, f: F) -> Vec<T> {
    let next = AtomicUsize::new(0);
    let (f, init, next) = (&f, &init, &next);
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads.min(n).max(1))
            .map(|_| {
                s.spawn(move || {
                    let mut st = init();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        f(&mut st, i);
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

const BLK: usize = 256;

/// Append to a list of fixed-capacity blocks.
#[inline]
fn bpush<T>(b: &mut Vec<Vec<T>>, x: T) {
    match b.last_mut() {
        Some(v) if v.len() < BLK => v.push(x),
        _ => {
            let mut v = Vec::with_capacity(BLK);
            v.push(x);
            b.push(v);
        }
    }
}

/// Bit masks of ',', '\n' and '"' over 64 bytes at p.
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::*;
        let (c, nl, q) = (_mm_set1_epi8(b',' as i8), _mm_set1_epi8(b'\n' as i8), _mm_set1_epi8(b'"' as i8));
        let (mut mc, mut mn, mut mq) = (0u64, 0u64, 0u64);
        for k in 0..4 {
            let v = _mm_loadu_si128(p.add(16 * k) as *const __m128i);
            mc |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, c)) as u32 as u64) << (16 * k);
            mn |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, nl)) as u32 as u64) << (16 * k);
            mq |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, q)) as u32 as u64) << (16 * k);
        }
        (mc, mn, mq)
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        masks_tail(std::slice::from_raw_parts(p, 64))
    }
}

#[inline]
fn masks_tail(b: &[u8]) -> (u64, u64, u64) {
    let (mut mc, mut mn, mut mq) = (0u64, 0u64, 0u64);
    for (i, &c) in b.iter().enumerate() {
        mc |= ((c == b',') as u64) << i;
        mn |= ((c == b'\n') as u64) << i;
        mq |= ((c == b'"') as u64) << i;
    }
    (mc, mn, mq)
}

/// Close field buf[fs..p] holding qc quotes (the rules.md dialect: a quoted field is
/// wrapped whole, nothing else holds a quote). False if malformed.
#[inline(always)]
fn close_field<'a, const N: usize>(buf: &'a [u8], fs: usize, p: usize, qc: u32, n: usize, fl: &mut [&'a [u8]; N]) -> bool {
    let v = if qc == 0 {
        &buf[fs..p]
    } else if qc == 2 && p - fs >= 2 && buf[fs] == b'"' && buf[p - 1] == b'"' {
        &buf[fs + 1..p - 1]
    } else {
        return false;
    };
    if n < N {
        fl[n] = v;
    }
    true
}

/// Call f(fields) for every data line of buf[start..end] that `split` accepts into N
/// fields, with identical results. Separators and quotes come from 64-byte masks.
#[inline]
fn for_rows<'a, const N: usize>(buf: &'a [u8], start: usize, end: usize, mut f: impl FnMut(&[&'a [u8]; N])) {
    let mut fl: [&'a [u8]; N] = [&[]; N];
    let mut n = 0usize; // fields closed in the current line
    let mut fs = start; // current field start
    let mut ls = start; // current line start
    let mut bad = false; // current line malformed
    let mut qc = 0u32; // quotes in the current field so far
    let mut pos = start;
    while pos < end {
        let blen = (end - pos).min(64);
        let (mc, mn, mq) = if blen == 64 { unsafe { masks64(buf.as_ptr().add(pos)) } } else { masks_tail(&buf[pos..end]) };
        let mut m = mc | mn;
        let mut done = 0u64; // quote bits already counted
        while m != 0 {
            let b = m.trailing_zeros() as usize;
            let bit = 1u64 << b;
            m &= m - 1;
            qc += (mq & (bit - 1) & !done).count_ones();
            done = bit | (bit - 1);
            let p = pos + b;
            if !close_field(buf, fs, p, qc, n, &mut fl) {
                bad = true;
            }
            n += 1;
            qc = 0;
            fs = p + 1;
            if mn & bit != 0 {
                if !bad && n == N {
                    f(&fl);
                }
                n = 0;
                bad = false;
                ls = p + 1;
            }
        }
        qc += (mq & !done).count_ones();
        pos += blen;
    }
    if ls < end {
        if !close_field(buf, fs, end, qc, n, &mut fl) {
            bad = true;
        }
        n += 1;
        if !bad && n == N {
            f(&fl);
        }
    }
}

#[inline(always)]
fn is8(a: &[u8], b: &[u8; 8]) -> bool {
    a.len() == 8 && u64::from_le_bytes(a.try_into().unwrap()) == u64::from_le_bytes(*b)
}

#[inline(always)]
fn is4(a: &[u8], b: &[u8; 4]) -> bool {
    a.len() == 4 && u32::from_le_bytes(a.try_into().unwrap()) == u32::from_le_bytes(*b)
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

/// Linked IDs: id -> person (byte-wise smallest id of its component).
/// IDs are sharded by hash; every shard owns a table and a dense index range.
const Q: usize = 64;

#[inline]
fn shard(h: u64) -> usize {
    ((h >> 52) as usize) & (Q - 1)
}

#[derive(Clone, Copy)]
struct LE {
    h: u64,
    key: [u8; 16],
    g: u32,
    len: u32,
}

#[inline]
fn key16(x: &[u8]) -> [u8; 16] {
    let mut k = [0u8; 16];
    let n = x.len().min(16);
    k[..n].copy_from_slice(&x[..n]);
    k
}

struct SyncPtr<T>(*mut T);
unsafe impl<T> Sync for SyncPtr<T> {}
unsafe impl<T> Send for SyncPtr<T> {}

const FBITS: u32 = 24;
const BATCH: usize = 64;

#[inline(always)]
fn prefetch<T>(x: &T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(x as *const T as *const i8, std::arch::x86_64::_MM_HINT_T0)
    };
}

/// Resolve-table slot: one cache-line half holds the key and the answer.
/// `hk` is the id hash with bit 0 set (0 marks an empty slot); `k8` is the id's
/// first 8 bytes, a second check on top of the 64-bit hash.
#[derive(Clone, Copy, Default)]
#[repr(C, align(32))]
struct RE {
    hk: u64,
    k8: u64,
    person: u64, // S
    ph: u64,
}

#[inline]
fn k8(x: &[u8]) -> u64 {
    let mut t = [0u8; 8];
    let n = x.len().min(8);
    t[..n].copy_from_slice(&x[..n]);
    u64::from_le_bytes(t)
}

struct Links {
    filter: Vec<u64>, // bit (h mod 2^FBITS) set for every linked id
    rtabs: Vec<Vec<RE>>, // per shard, power-of-two open addressing, linear probe
    n: usize,
}

impl Links {
    fn build(bufs: &[&[u8]], chunks: &[(usize, usize, usize)], threads: usize) -> Self {
        // 1. parse: per chunk, edges count and per-shard endpoints (hash, id, local slot)
        let parsed: Vec<(usize, Vec<Vec<(u64, S, u32)>>)> = par_map(chunks.len(), threads, |c| {
            let (k, s, e) = chunks[c];
            let mut per: Vec<Vec<(u64, S, u32)>> = (0..Q).map(|_| Vec::new()).collect();
            let mut ne = 0u32;
            for_lines(bufs[k], s, e, |line| {
                let mut f: [&[u8]; 2] = [&[]; 2];
                if split(line, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
                    for side in 0..2 {
                        let h = hash(f[side]);
                        per[shard(h)].push((h, S::new(f[side]), ne * 2 + side as u32));
                    }
                    ne += 1;
                }
            });
            (ne as usize, per)
        });
        let mut eoff = Vec::with_capacity(parsed.len() + 1);
        eoff.push(0usize);
        for x in &parsed {
            eoff.push(eoff.last().unwrap() + x.0);
        }
        let nedge = *eoff.last().unwrap();
        // 2. per shard: dedupe ids, local index; endpoint slot -> (shard << 26) | local
        let mut ep: Vec<u32> = vec![0; nedge * 2];
        let epp = SyncPtr(ep.as_mut_ptr());
        let epp = &epp;
        let shards: Vec<(HashTable<LE>, Vec<S>, Vec<u64>)> = par_map(Q, threads, |q| {
            let cnt: usize = parsed.iter().map(|x| x.1[q].len()).sum();
            let mut t: HashTable<LE> = HashTable::with_capacity(cnt);
            let mut ids: Vec<S> = Vec::with_capacity(cnt);
            let mut hs: Vec<u64> = Vec::with_capacity(cnt);
            for (c, x) in parsed.iter().enumerate() {
                for &(h, id, slot) in &x.1[q] {
                    let b = id.get();
                    let key = key16(b);
                    let len = b.len() as u32;
                    let l = match t.find(h, |e| {
                        e.h == h && e.len == len && e.key == key && (len <= 16 || ids[e.g as usize].get() == b)
                    }) {
                        Some(e) => e.g,
                        None => {
                            let l = ids.len() as u32;
                            ids.push(id);
                            hs.push(h);
                            t.insert_unique(h, LE { h, key, g: l, len }, |e| e.h);
                            l
                        }
                    };
                    unsafe { *epp.0.add(eoff[c] * 2 + slot as usize) = ((q as u32) << 26) | l };
                }
            }
            (t, ids, hs)
        });
        let mut soff = vec![0u32; Q + 1];
        for q in 0..Q {
            soff[q + 1] = soff[q] + shards[q].1.len() as u32;
        }
        let n = soff[Q] as usize;
        // 3. union-find over dense ids
        fn find(p: &mut [u32], mut x: u32) -> u32 {
            while p[x as usize] != x {
                let g = p[p[x as usize] as usize];
                p[x as usize] = g;
                x = g;
            }
            x
        }
        let gid = |v: u32| soff[(v >> 26) as usize] + (v & ((1 << 26) - 1));
        let mut parent: Vec<u32> = (0..n as u32).collect();
        for e in ep.chunks_exact(2) {
            let (ra, rb) = (find(&mut parent, gid(e[0])), find(&mut parent, gid(e[1])));
            if ra != rb {
                parent[ra.max(rb) as usize] = ra.min(rb);
            }
        }
        drop(ep);
        let mut ids: Vec<S> = Vec::with_capacity(n);
        let mut hs: Vec<u64> = Vec::with_capacity(n);
        let mut tabs = Vec::with_capacity(Q);
        for (t, i, h) in shards {
            ids.extend_from_slice(&i);
            hs.extend_from_slice(&h);
            tabs.push(t);
        }
        // 4. person = byte-wise smallest id of the component
        let bestc: Vec<std::sync::atomic::AtomicU32> = (0..n as u32).map(std::sync::atomic::AtomicU32::new).collect();
        {
            let par = &parent[..];
            let step = n.div_ceil(threads * 8).max(1);
            par_map(n.div_ceil(step), threads, |c| {
                for i in c * step..((c + 1) * step).min(n) {
                    let mut r = i as u32;
                    while par[r as usize] != r {
                        r = par[r as usize];
                    }
                    let r = r as usize;
                    if r != i {
                        let mine = ids[i].get();
                        let mut cur = bestc[r].load(Ordering::Relaxed);
                        while mine < ids[cur as usize].get() {
                            match bestc[r].compare_exchange_weak(cur, i as u32, Ordering::Relaxed, Ordering::Relaxed) {
                                Ok(_) => break,
                                Err(x) => cur = x,
                            }
                        }
                    }
                }
            });
        }
        let best: Vec<u32> = bestc.into_iter().map(|a| a.into_inner()).collect();
        drop(tabs);
        let rtabs: Vec<Vec<RE>> = par_map(Q, threads, |q| {
            let (a, b) = (soff[q] as usize, soff[q + 1] as usize);
            let size = ((b - a) * 10 / 7 + 1).next_power_of_two().max(16);
            let mask = size - 1;
            let mut t = vec![RE::default(); size];
            for i in a..b {
                let h = hs[i];
                let mut x = i as u32;
                while parent[x as usize] != x {
                    x = parent[x as usize];
                }
                let r = best[x as usize] as usize;
                let mut j = (h >> 24) as usize & mask;
                while t[j].hk != 0 {
                    j = (j + 1) & mask;
                }
                t[j] = RE { hk: h | 1, k8: k8(ids[i].get()), person: ids[r].0, ph: hs[r] };
            }
            t
        });
        let mut filter = vec![0u64; 1 << (FBITS - 6)];
        for &h in &hs {
            let b = (h as usize) & ((1 << FBITS) - 1);
            filter[b >> 6] |= 1 << (b & 63);
        }
        Links { filter, rtabs, n }
    }

    /// (person, hash(person))
    /// xs[i] = (uid, _) in, (person, hash(person)) out. Up to BATCH entries.
    /// Three passes so the filter and table cache misses overlap.
    #[inline]
    fn resolve_batch(&self, xs: &mut [(S, u64)]) {
        let n = xs.len();
        let mut hs = [0u64; BATCH];
        for i in 0..n {
            let h = hash(xs[i].0.get());
            hs[i] = h;
            let b = (h as usize) & ((1 << FBITS) - 1);
            prefetch(&self.filter[b >> 6]);
        }
        let mut hit = [false; BATCH];
        for i in 0..n {
            let h = hs[i];
            let b = (h as usize) & ((1 << FBITS) - 1);
            if self.filter[b >> 6] & (1 << (b & 63)) != 0 {
                hit[i] = true;
                let t = &self.rtabs[shard(h)];
                prefetch(&t[(h >> 24) as usize & (t.len() - 1)]);
            }
        }
        for i in 0..n {
            let h = hs[i];
            xs[i] = if hit[i] { self.probe(xs[i].0.get(), h) } else { (xs[i].0, h) };
        }
    }

    #[inline]
    fn probe(&self, uid: &[u8], h: u64) -> (S, u64) {
        let t = &self.rtabs[shard(h)];
        let mask = t.len() - 1;
        let (hk, k) = (h | 1, k8(uid));
        let mut j = (h >> 24) as usize & mask;
        loop {
            let e = &t[j];
            if e.hk == hk && e.k8 == k {
                return (S(e.person), e.ph);
            }
            if e.hk == 0 {
                return (S::new(uid), h);
            }
            j = (j + 1) & mask;
        }
    }
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct EvA {
    h: u64,
    eid: S,
    person: S,
    ph: u64,
    ing: i64,
    ts: i64,
    price: u32, // > 0 only for a valid purchase
    cat: u16,
    ok: bool,
}

#[derive(Clone, Copy)]
struct EvB {
    ph: u64,
    person: S,
    ts: i64,
    price: u32,
    cat: u16,
}

#[derive(Clone, Copy)]
struct Clk {
    ph: u64,
    cid: S,
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
    fn person(&mut self, person: &'a [u8], ev: &[EvB], ck: &[Clk]) {
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
    let props = Arc::new(
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
            .build(),
    );
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

/// A ByteArray that borrows `b` without copying. Every caller's bytes (mmapped
/// input, category names in main) stay alive until the process exits.
#[inline]
fn borrow_ba(b: &[u8]) -> ByteArray {
    ByteArray::from(bytes::Bytes::from_static(unsafe { std::mem::transmute::<&[u8], &'static [u8]>(b) }))
}

fn write_ud(path: &Path, rows: &[Ud], cats: &[Vec<u8>]) {
    write_parquet(path, UD_SCHEMA, |cols| {
        cols.push(ColData::Str(rows.iter().map(|r| borrow_ba(r.person)).collect(), None));
        cols.push(ColData::I32(rows.iter().map(|r| r.day).collect()));
        cols.push(ColData::I64(rows.iter().map(|r| r.sessions).collect()));
        cols.push(ColData::I64(rows.iter().map(|r| r.events).collect()));
        cols.push(ColData::I64(rows.iter().map(|r| r.revenue).collect()));
        let def: Vec<i16> = rows.iter().map(|r| r.top.is_some() as i16).collect();
        let vals: Vec<ByteArray> =
            rows.iter().filter_map(|r| r.top.map(|c| borrow_ba(&cats[c as usize]))).collect();
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
    let log = |m: &str| {
        let rss = fs::read_to_string("/proc/self/statm")
            .ok()
            .and_then(|x| x.split(' ').nth(1).and_then(|v| v.parse::<usize>().ok()))
            .unwrap_or(0)
            >> 8;
        eprintln!("[{:7.3}s] {m} (arena {} MB, rss {rss} MB)", t0.elapsed().as_secs_f64(), bump::used_mb())
    };
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()));

    if std::env::var_os("TS_CHECK").is_some() {
        let mut n = 0u64;
        let mut x = 0x1234_5678_9abc_def0u64;
        let mut check = |t: &[u8]| {
            n += 1;
            assert_eq!(ts(t), ts_ref(t), "{}", String::from_utf8_lossy(t));
        };
        for y in (0..=9999).step_by(7).chain(1965..2040) {
            for mo in 0..=13 {
                for d in [0, 1, 2, 15, 28, 29, 30, 31, 32] {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let (h, mi, s) = (x % 25, (x >> 8) % 61, (x >> 16) % 61);
                    let (oh, om) = ((x >> 24) % 25, (x >> 32) % 61);
                    check(format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z").as_bytes());
                    check(format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}+{oh:02}:{om:02}").as_bytes());
                    check(format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}-{oh:02}:{om:02}").as_bytes());
                }
            }
        }
        for t in ["", "0", "000", "1760000000000", "0001760000000000", "1234567890123456", "999999999999999",
            "12a", "-5", "2026-10-03T19:58:10Z", "2026-10-03 19:58:10Z", "2026-10-03T19:58:1aZ", "2026-1x-03T19:58:10Z",
            "2026-10-03T19:58:10+0a:00", "2026-10-03T19:58:10*05:00", "00000000000000000000", "2026-10-03T19:58:10.000Z"] {
            check(t.as_bytes());
        }
        let al = b"0123456789000000123456789:/aT-Z+ ";
        for _ in 0..2_000_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let mut t = [0u8; 13];
            for (i, c) in t.iter_mut().enumerate() {
                let r = (x >> (i * 4)) as usize;
                *c = if r & 0x30 == 0 { al[r % al.len()] } else { b'0' + (r % 10) as u8 };
            }
            check(&t);
            let mut t = *b"2027-12-18T22:22:54Z";
            let k = (x >> 52) as usize % 20;
            t[k] = al[(x >> 40) as usize % al.len()];
            check(&t);
        }
        eprintln!("TS_CHECK ok: {n} strings");
        return;
    }
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

    // ---- links (all threads), then events + clicks (one pool) ----
    let links = Links::build(&bufs, &ln_chunks, threads);
    log(&format!("links: {} ids", links.n));
    let parse_events = |out: &mut Vec<Vec<Vec<EvA>>>, i: usize| {
        let (k, s, e) = ev_chunks[i];
        let mut pend: Vec<EvA> = Vec::with_capacity(BATCH);
        let mut keys: Vec<(S, u64)> = Vec::with_capacity(BATCH);
        let flush = |pend: &mut Vec<EvA>, keys: &mut Vec<(S, u64)>, out: &mut Vec<Vec<Vec<EvA>>>| {
            links.resolve_batch(keys);
            let mut j = 0;
            for mut e in pend.drain(..) {
                if e.ok {
                    (e.person, e.ph) = keys[j];
                    j += 1;
                }
                bpush(&mut out[dbucket(e.h)], e);
            }
            keys.clear();
        };
        for_rows::<7>(bufs[k], s, e, |f| {
            if f[0].is_empty() {
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
            if is8(et, b"purchase") {
                p = price(f[6]);
                if p == 0 {
                    ok = false;
                } else {
                    cat = prod.find(hash(f[5]), |x| x.0 == f[5]).map_or(unknown, |x| x.1);
                }
            } else if !is4(et, b"view") && !is4(et, b"cart") {
                ok = false;
            }
            if ok && ing.saturating_sub(t) > DAY {
                ok = false;
            }
            let h = hash(f[0]);
            if ok {
                keys.push((S::new(f[1]), 0));
            }
            pend.push(EvA { h, eid: S::new(f[0]), person: S(0), ph: 0, ing, ts: t, price: p, cat, ok });
            if pend.len() == BATCH {
                flush(&mut pend, &mut keys, out);
            }
        });
        flush(&mut pend, &mut keys, out);
    };
    let parse_clicks = |out: &mut Vec<Vec<Vec<Clk>>>, i: usize| {
        let (k, s, e) = ck_chunks[i];
        let mut pend: Vec<Clk> = Vec::with_capacity(BATCH);
        let mut keys: Vec<(S, u64)> = Vec::with_capacity(BATCH);
        let flush = |pend: &mut Vec<Clk>, keys: &mut Vec<(S, u64)>, out: &mut Vec<Vec<Vec<Clk>>>| {
            links.resolve_batch(keys);
            for (mut c, &(_, ph)) in pend.drain(..).zip(keys.iter()) {
                c.ph = ph;
                bpush(&mut out[bucket(ph)], c);
            }
            keys.clear();
        };
        for_rows::<4>(bufs[k], s, e, |f| {
            if f[0].is_empty() || f[1].is_empty() {
                return;
            }
            let t = match ts(f[3]) {
                Some(t) if t >= ws - WEEK && t < we => t,
                _ => return,
            };
            let Some(&(_, c)) = camp.find(hash(f[2]), |x| x.0 == f[2]) else { return };
            keys.push((S::new(f[1]), 0));
            pend.push(Clk { ph: 0, cid: S::new(f[0]), ts: t, camp: c });
            if pend.len() == BATCH {
                flush(&mut pend, &mut keys, out);
            }
        });
        flush(&mut pend, &mut keys, out);
    };
    let nev = ev_chunks.len();
    // per worker: (event buckets by event_id hash, click buckets by person hash), each a block list
    let states: Vec<(Vec<Vec<Vec<EvA>>>, Vec<Vec<Vec<Clk>>>)> = par_states(
        nev + ck_chunks.len(),
        threads,
        || ((0..PD).map(|_| Vec::new()).collect(), (0..P).map(|_| Vec::new()).collect()),
        |st, i| {
            if i < nev {
                parse_events(&mut st.0, i)
            } else {
                parse_clicks(&mut st.1, i - nev)
            }
        },
    );
    log("parsed events + clicks");

    // ---- dedup events (smallest ingest_ts wins); count survivors per person bucket ----
    let parts_of = |p: usize| -> Vec<&[EvA]> { states.iter().flat_map(|st| st.0[p].iter().map(|v| &v[..])).collect() };
    let deds: Vec<(Vec<bool>, [u32; P])> = par_map_init(
        PD,
        threads,
        HashTable::<(u64, i64, u32)>::new,
        |t, p| {
            let parts = parts_of(p);
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
            t.clear();
            t.reserve(total, |x| x.0);
            let mut lose = vec![false; total];
            let mut gi = 0u32;
            for q in &parts {
                for e in q.iter() {
                    match t.find_mut(e.h, |x| x.0 == e.h && get(x.2).eid.get() == e.eid.get()) {
                        Some(x) => {
                            if e.ing < x.1 {
                                lose[x.2 as usize] = true;
                                x.1 = e.ing;
                                x.2 = gi;
                            } else {
                                lose[gi as usize] = true;
                            }
                        }
                        None => {
                            t.insert_unique(e.h, (e.h, e.ing, gi), |x| x.0);
                        }
                    }
                    gi += 1;
                }
            }
            let mut cnt = [0u32; P];
            let mut gi = 0usize;
            for q in &parts {
                for e in q.iter() {
                    if e.ok && !lose[gi] {
                        cnt[bucket(e.ph)] += 1;
                    }
                    gi += 1;
                }
            }
            (lose, cnt)
        },
    );

    // ---- survivors -> evq[person bucket], each dedup bucket writing its own precomputed range ----
    let mut start = vec![[0usize; P]; PD];
    let mut tot = [0usize; P];
    for p in 0..PD {
        for q in 0..P {
            start[p][q] = tot[q];
            tot[q] += deds[p].1[q] as usize;
        }
    }
    let mut evq: Vec<Vec<EvB>> = (0..P).map(|q| Vec::with_capacity(tot[q])).collect();
    let dst: Vec<SyncPtr<EvB>> = evq.iter_mut().map(|v| SyncPtr(v.as_mut_ptr())).collect();
    // Debug only: SESSIONS_DUMP=dir writes each surviving event as event_id,person.
    let dump = std::env::var_os("SESSIONS_DUMP");
    par_map(PD, threads, |p| {
        let parts = parts_of(p);
        let lose = &deds[p].0;
        let mut pos = start[p];
        let mut dbuf = Vec::new();
        let mut gi = 0usize;
        for q in &parts {
            for e in q.iter() {
                if e.ok && !lose[gi] {
                    let b = bucket(e.ph);
                    unsafe { dst[b].0.add(pos[b]).write(EvB { ph: e.ph, person: e.person, ts: e.ts, price: e.price, cat: e.cat }) };
                    pos[b] += 1;
                    if dump.is_some() {
                        dbuf.extend_from_slice(e.eid.get());
                        dbuf.push(b',');
                        dbuf.extend_from_slice(e.person.get());
                        dbuf.push(10); // newline
                    }
                }
                gi += 1;
            }
        }
        if let Some(d) = &dump {
            fs::write(PathBuf::from(d).join(format!("s-{p}.txt")), &dbuf).unwrap();
        }
    });
    for (q, v) in evq.iter_mut().enumerate() {
        unsafe { v.set_len(tot[q]) };
    }
    log("dedup done");

    // ---- per person (a person is identified by its 64-bit name hash) ----
    let ncat = cats.len();
    let evq: Vec<std::sync::Mutex<Vec<EvB>>> = evq.into_iter().map(std::sync::Mutex::new).collect();
    const SB: usize = 1 << 16;
    let sub = |ph: u64| (ph & (SB as u64 - 1)) as usize;
    let maxev = tot.iter().copied().max().unwrap_or(0);
    let ctxs: Vec<Ctx> = par_map_init(
        P,
        threads,
        || (Vec::<EvB>::with_capacity(maxev), vec![0u32; SB + 1], vec![0u32; SB + 1], vec![0u32; SB]),
        |buf, p| {
            let (es, eo, co, pos) = buf;
            let src = evq[p].lock().unwrap();
            let n = src.len();
            eo.fill(0);
            for e in src.iter() {
                eo[sub(e.ph) + 1] += 1;
            }
            for g in 0..SB {
                eo[g + 1] += eo[g];
            }
            pos.copy_from_slice(&eo[..SB]);
            es.clear();
            let d = es.as_mut_ptr();
            for e in src.iter() {
                let g = sub(e.ph);
                unsafe { d.add(pos[g] as usize).write(*e) };
                pos[g] += 1;
            }
            unsafe { es.set_len(n) };
            drop(src);

            co.fill(0);
            let mut nck = 0usize;
            for st in &states {
                for v in &st.1[p] {
                    nck += v.len();
                    for c in v {
                        co[sub(c.ph) + 1] += 1;
                    }
                }
            }
            for g in 0..SB {
                co[g + 1] += co[g];
            }
            pos.copy_from_slice(&co[..SB]);
            let mut cks: Vec<Clk> = Vec::with_capacity(nck);
            let d = cks.as_mut_ptr();
            for st in &states {
                for v in &st.1[p] {
                    for c in v {
                        let g = sub(c.ph);
                        unsafe { d.add(pos[g] as usize).write(*c) };
                        pos[g] += 1;
                    }
                }
            }
            unsafe { cks.set_len(nck) };

            let mut ctx = Ctx::new(ncamp, ncat, ndays, day0);
            for g in 0..SB {
                let (ea, eb) = (eo[g] as usize, eo[g + 1] as usize);
                if ea == eb {
                    continue;
                }
                let ev = &mut es[ea..eb];
                ev.sort_unstable_by_key(|e| (e.ph, e.ts));
                let ck = &mut cks[co[g] as usize..co[g + 1] as usize];
                ck.sort_unstable_by(|a, b| (a.ph, a.ts).cmp(&(b.ph, b.ts)).then_with(|| a.cid.get().cmp(b.cid.get())));
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
                    ctx.person(ev[i].person.get(), &ev[i..ie], &ck[j..je]);
                    i = ie;
                    j = je;
                }
            }
            ctx
        },
    );
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
    std::process::exit(0);
}
