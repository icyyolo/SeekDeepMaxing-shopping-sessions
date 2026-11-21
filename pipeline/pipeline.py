#!/usr/bin/env python3
"""
Shopping Sessions: DuckDB pipeline.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)

Every CSV is read as raw lines (DuckDB's own dialect handling differs from
rules.md) and split/validated in SQL. Files holding a '\\r' are split in Python,
because DuckDB treats '\\r' as a line ending and rules.md does not.
"""
import glob
import json
import mmap
import os
import sys
import time
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

T0 = time.time()
DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

DAY = 86_400_000
WEEK = 7 * DAY


def log(msg):
    print(f"[{time.time() - T0:6.2f}s] {msg}", file=sys.stderr, flush=True)


con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.8))}GB'")
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")

with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)


def iso_to_ms(s):
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)


WS = iso_to_ms(manifest["window_start"])
WE = iso_to_ms(manifest["window_end"])

# --------------------------------------------------------------------------
# Macros
# --------------------------------------------------------------------------
# A field is either wrapped whole in quotes or holds no quote; no value holds a comma.
FIELD = '(?:"[^",]*"|[^",]*)'


def line_pattern(n):
    return "^" + FIELD + "(?:," + FIELD + "){" + str(n - 1) + "}$"


con.execute("""
CREATE MACRO fields(line) AS
  list_transform(string_split(line, ','),
                 x -> nullif(CASE WHEN x[1] = '"' THEN x[2:-2] ELSE x END, ''))
""")

# days_from_civil (Howard Hinnant); years 0000..9999 are all >= 0 except year-0 Jan/Feb,
# which is far outside any window anyway.
con.execute("""
CREATE MACRO civil_days(y, m, d) AS (
  ((y - (m <= 2)::INT) // 400) * 146097
  + ((y - (m <= 2)::INT) % 400) * 365
  + ((y - (m <= 2)::INT) % 400) // 4
  - ((y - (m <= 2)::INT) % 400) // 100
  + (153 * (m + CASE WHEN m > 2 THEN -3 ELSE 9 END) + 2) // 5 + d - 1
  - 719468)
""")

con.execute("""
CREATE MACRO dim(y, m) AS CASE
  WHEN m = 2 THEN CASE WHEN (y % 4 = 0 AND y % 100 <> 0) OR y % 400 = 0 THEN 29 ELSE 28 END
  WHEN m IN (4, 6, 9, 11) THEN 30 ELSE 31 END
""")

con.execute("""
CREATE MACRO iso_ok(y, mo, d, h, mi, se, oh, om) AS
  mo BETWEEN 1 AND 12 AND d BETWEEN 1 AND dim(y, mo) AND h < 24 AND mi < 60 AND se < 60
  AND oh < 24 AND om < 60
""")

con.execute("""
CREATE MACRO iso_val(y, mo, d, h, mi, se, sg, oh, om) AS
  (((civil_days(y, mo, d) * 24 + h) * 60 + mi) * 60 + se) * 1000 - sg * (oh * 60 + om) * 60000
""")

# Integer ms since epoch, or NULL for a bad timestamp (window checked by the caller).
con.execute("""
CREATE MACRO ts_ms(s) AS CASE
  WHEN s IS NULL THEN NULL
  WHEN regexp_full_match(s, '[0-9]+') THEN
    CASE WHEN length(ltrim(s, '0')) <= 18 THEN TRY_CAST(s AS BIGINT) END
  WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})') THEN
    CASE WHEN iso_ok(TRY_CAST(s[1:4] AS BIGINT), TRY_CAST(s[6:7] AS BIGINT), TRY_CAST(s[9:10] AS BIGINT),
                     TRY_CAST(s[12:13] AS BIGINT), TRY_CAST(s[15:16] AS BIGINT), TRY_CAST(s[18:19] AS BIGINT),
                     CASE WHEN s[20] = 'Z' THEN 0 ELSE TRY_CAST(s[21:22] AS BIGINT) END,
                     CASE WHEN s[20] = 'Z' THEN 0 ELSE TRY_CAST(s[24:25] AS BIGINT) END)
    THEN iso_val(TRY_CAST(s[1:4] AS BIGINT), TRY_CAST(s[6:7] AS BIGINT), TRY_CAST(s[9:10] AS BIGINT),
                 TRY_CAST(s[12:13] AS BIGINT), TRY_CAST(s[15:16] AS BIGINT), TRY_CAST(s[18:19] AS BIGINT),
                 CASE WHEN s[20] = '-' THEN -1 ELSE 1 END,
                 CASE WHEN s[20] = 'Z' THEN 0 ELSE TRY_CAST(s[21:22] AS BIGINT) END,
                 CASE WHEN s[20] = 'Z' THEN 0 ELSE TRY_CAST(s[24:25] AS BIGINT) END)
    END
  END
""")


# --------------------------------------------------------------------------
# Raw lines
# --------------------------------------------------------------------------
def has_cr(path):
    if os.path.getsize(path) == 0:
        return False
    with open(path, "rb") as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as m:
        return m.find(b"\r") != -1


def raw_lines(sub, name):
    """Create view `name` with one VARCHAR column `line` (NULL for an empty line)."""
    files = sorted(glob.glob(os.path.join(DATA, sub, "*.csv")))
    cr = {p for p in files if has_cr(p)}
    plain = [p for p in files if p not in cr]
    parts = []
    if plain:
        lst = ", ".join("'" + p.replace("'", "''") + "'" for p in plain)
        parts.append(f"""SELECT line FROM read_csv([{lst}],
            columns={{'line': 'VARCHAR'}}, delim='\x01', quote='', escape='',
            header=true, strict_mode=false, auto_detect=false)""")
    if cr:
        lines = []
        for p in sorted(cr):
            with open(p, "rb") as f:
                chunks = f.read().split(b"\n")
            if chunks and chunks[-1] == b"":
                chunks.pop()
            lines.extend(c.decode("utf-8", "replace") or None for c in chunks[1:])
        df = pd.DataFrame({"line": pd.Series(lines, dtype=object)})
        con.register(f"{name}_cr", df)
        parts.append(f"SELECT line FROM {name}_cr")
    if not parts:
        parts.append("SELECT NULL::VARCHAR AS line WHERE false")
    con.execute(f"CREATE OR REPLACE VIEW {name} AS " + " UNION ALL ".join(parts))
    log(f"{sub}: {len(files)} files, {len(cr)} with CR")


raw_lines("events", "raw_events")
raw_lines("clicks", "raw_clicks")
raw_lines("links", "raw_links")

con.execute(f"""CREATE TABLE products AS SELECT * FROM read_csv('{DATA}/products.csv',
    header=true, all_varchar=true)""")
con.execute(f"""CREATE TABLE campaigns AS SELECT * FROM read_csv('{DATA}/campaigns.csv',
    header=true, all_varchar=true)""")

# --------------------------------------------------------------------------
# Linked IDs -> person name (byte-wise smallest ID of the component)
# --------------------------------------------------------------------------
con.execute(f"""
CREATE TABLE link_edges AS
SELECT f[1] AS a, f[2] AS b FROM (
  SELECT fields(line) AS f FROM raw_links
  WHERE regexp_full_match(line, '{line_pattern(2)}'))
WHERE f[1] IS NOT NULL AND f[2] IS NOT NULL
""")
con.execute("""
CREATE TABLE link_ids AS
SELECT id, (row_number() OVER (ORDER BY id) - 1)::BIGINT AS idx
FROM (SELECT a AS id FROM link_edges UNION SELECT b FROM link_edges)
""")
n_ids = con.execute("SELECT count(*) FROM link_ids").fetchone()[0]
if n_ids:
    e = con.execute("""
        SELECT ia.idx AS s, ib.idx AS d FROM link_edges
        JOIN link_ids ia ON ia.id = a JOIN link_ids ib ON ib.id = b
    """).fetchnumpy()
    g = coo_matrix((np.ones(len(e["s"]), dtype=np.int8), (e["s"], e["d"])), shape=(n_ids, n_ids))
    _, labels = connected_components(g, directed=False)
    root = np.full(labels.max() + 1, n_ids, dtype=np.int64)
    np.minimum.at(root, labels, np.arange(n_ids, dtype=np.int64))
    person_idx = pd.DataFrame({"idx": np.arange(n_ids, dtype=np.int64), "ridx": root[labels]})
else:
    person_idx = pd.DataFrame({"idx": np.array([], dtype=np.int64), "ridx": np.array([], dtype=np.int64)})
con.register("person_idx", person_idx)
con.execute("""
CREATE TABLE idmap AS
SELECT l.id, r.id AS person FROM person_idx p
JOIN link_ids l ON l.idx = p.idx JOIN link_ids r ON r.idx = p.ridx
""")
log(f"links: {n_ids} ids")

# --------------------------------------------------------------------------
# Events
# --------------------------------------------------------------------------
con.execute(f"""
CREATE TABLE ev AS
WITH parsed AS (
  SELECT fields(line) AS f FROM raw_events
  WHERE regexp_full_match(line, '{line_pattern(7)}')
), kept AS (
  SELECT arg_min({{'uid': f[2], 'ts': f[3], 'et': f[5], 'pid': f[6], 'price': f[7], 'ing': ts_ms(f[4])}},
                 ts_ms(f[4])) AS r
  FROM parsed WHERE f[1] IS NOT NULL
  GROUP BY f[1]
), typed AS (
  SELECT r.uid, ts_ms(r.ts) AS ts, r.ing, r.et, r.pid,
         CASE WHEN r.et = 'purchase' AND regexp_full_match(r.price, '[0-9]+')
                   AND length(ltrim(r.price, '0')) <= 8
              THEN TRY_CAST(r.price AS BIGINT) END AS price
  FROM kept
)
SELECT coalesce(m.person, t.uid) AS person, t.ts,
       t.et = 'purchase' AS is_purchase,
       CASE WHEN t.et = 'purchase' THEN t.price ELSE 0 END AS price,
       coalesce(p.category, 'unknown') AS category
FROM typed t
LEFT JOIN idmap m ON m.id = t.uid
LEFT JOIN products p ON p.product_id = t.pid
WHERE t.uid IS NOT NULL AND t.pid IS NOT NULL
  AND t.ts IS NOT NULL AND t.ts >= {WS} AND t.ts < {WE}
  AND t.et IN ('view', 'cart', 'purchase')
  AND (t.et <> 'purchase' OR (t.price BETWEEN 1 AND 10000000))
  AND t.ing - t.ts <= {DAY}
""")
log(f"events: {con.execute('SELECT count(*) FROM ev').fetchone()[0]} kept")

# Sessionize: new session when the gap to the previous (distinct) ts exceeds 30 min.
con.execute("""
CREATE TABLE ev_s AS
SELECT *, sum(CASE WHEN prev IS NULL OR ts - prev > 1800000 THEN 1 ELSE 0 END)
            OVER (PARTITION BY person ORDER BY ts ROWS UNBOUNDED PRECEDING) AS sn
FROM (SELECT *, lag(ts) OVER (PARTITION BY person ORDER BY ts) AS prev FROM ev)
""")
con.execute(f"""
CREATE TABLE sess AS
SELECT person, sn, min(ts) AS start,
       DATE '1970-01-01' + CAST(min(ts) // {DAY} AS INTEGER) AS day,
       count(*) AS n_events, sum(price) AS revenue, bool_or(is_purchase) AS purchased
FROM ev_s GROUP BY person, sn
""")
log(f"sessions: {con.execute('SELECT count(*) FROM sess').fetchone()[0]}")

# --------------------------------------------------------------------------
# Clicks (copies are identical, so dedup changes nothing below and is skipped)
# --------------------------------------------------------------------------
con.execute(f"""
CREATE TABLE clk AS
WITH parsed AS (
  SELECT fields(line) AS f FROM raw_clicks
  WHERE regexp_full_match(line, '{line_pattern(4)}')
), typed AS (
  SELECT f[1] AS cid, f[2] AS uid, f[3] AS campaign, ts_ms(f[4]) AS ts FROM parsed
  WHERE f[1] IS NOT NULL AND f[2] IS NOT NULL
)
SELECT coalesce(m.person, t.uid) AS person, t.cid, t.campaign, t.ts
FROM typed t
JOIN campaigns c ON c.campaign_id = t.campaign
LEFT JOIN idmap m ON m.id = t.uid
WHERE t.ts IS NOT NULL AND t.ts >= {WS - WEEK} AND t.ts < {WE}
""")
log(f"clicks: {con.execute('SELECT count(*) FROM clk').fetchone()[0]} valid")

# Credit: latest click at or before the start (ties: smallest click_id), within 7 days.
con.execute(f"""
CREATE TABLE credit AS
WITH cb AS (
  SELECT person, ts, arg_min(campaign, cid) AS campaign FROM clk GROUP BY person, ts
)
SELECT s.*, CASE WHEN cb.ts IS NOT NULL AND s.start - cb.ts <= {WEEK}
                 THEN cb.campaign ELSE 'organic' END AS campaign
FROM sess s ASOF LEFT JOIN cb ON s.person = cb.person AND s.start >= cb.ts
""")

# Assists: per (person, campaign) merge click intervals [ts, ts + 7d], then map each
# interval to the range of the person's sessions (ranked by start) it covers.
con.execute("""
CREATE TABLE sess_r AS
SELECT person, day, start, row_number() OVER (PARTITION BY person ORDER BY start) AS r FROM sess
""")
con.execute(f"""
CREATE TABLE assists AS
WITH c AS (
  SELECT DISTINCT person, campaign, ts FROM clk
), g AS (
  SELECT *, sum(CASE WHEN prev IS NULL OR ts - prev > {WEEK} THEN 1 ELSE 0 END)
              OVER (PARTITION BY person, campaign ORDER BY ts ROWS UNBOUNDED PRECEDING) AS grp
  FROM (SELECT *, lag(ts) OVER (PARTITION BY person, campaign ORDER BY ts) AS prev FROM c)
), iv AS (
  SELECT person, campaign, min(ts) AS lo, max(ts) + {WEEK} AS hi FROM g GROUP BY person, campaign, grp
), lo_r AS (
  SELECT iv.*, s.r AS r_lo FROM iv ASOF JOIN sess_r s ON iv.person = s.person AND iv.lo <= s.start
), both_r AS (
  SELECT l.person, l.campaign, l.r_lo, s.r AS r_hi
  FROM lo_r l ASOF JOIN sess_r s ON l.person = s.person AND l.hi >= s.start
), pairs AS (
  SELECT DISTINCT person, campaign, unnest(range(r_lo, r_hi + 1)) AS r FROM both_r WHERE r_lo <= r_hi
)
SELECT p.campaign, s.day FROM pairs p JOIN sess_r s ON s.person = p.person AND s.r = p.r
""")
log("credit + assists done")

# --------------------------------------------------------------------------
# Outputs
# --------------------------------------------------------------------------
os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)

con.execute(f"""
COPY (
  WITH cat AS (
    SELECT e.person, s.day, e.category, sum(e.price) AS amt
    FROM ev_s e JOIN sess s USING (person, sn)
    WHERE e.is_purchase GROUP BY ALL
  ), top AS (
    SELECT person, day, category FROM cat
    QUALIFY row_number() OVER (PARTITION BY person, day ORDER BY amt DESC, category) = 1
  ), ud AS (
    SELECT person, day, count(*) AS sessions, sum(n_events) AS events, sum(revenue) AS revenue_cents
    FROM sess GROUP BY person, day
  )
  SELECT ud.person AS user_id, ud.day, ud.sessions::BIGINT AS sessions, ud.events::BIGINT AS events,
         ud.revenue_cents::BIGINT AS revenue_cents, top.category AS top_category
  FROM ud LEFT JOIN top USING (person, day)
) TO '{os.path.join(OUT, "user_daily", "part-0.parquet")}' (FORMAT parquet)
""")

con.execute(f"""
COPY (
  WITH cr AS (
    SELECT campaign, day, count(*) AS sessions, count(*) FILTER (WHERE purchased) AS purchasing_sessions,
           sum(revenue) AS revenue_cents
    FROM credit GROUP BY campaign, day
  ), asst AS (
    SELECT campaign, day, count(*) AS assisted_sessions FROM assists GROUP BY campaign, day
  )
  SELECT coalesce(cr.campaign, asst.campaign) AS campaign_id,
         CASE WHEN coalesce(cr.campaign, asst.campaign) = 'organic' THEN 'organic' ELSE c.channel END AS channel,
         coalesce(cr.day, asst.day) AS day,
         coalesce(cr.sessions, 0)::BIGINT AS sessions,
         coalesce(cr.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
         coalesce(cr.revenue_cents, 0)::BIGINT AS revenue_cents,
         coalesce(asst.assisted_sessions, 0)::BIGINT AS assisted_sessions
  FROM cr FULL OUTER JOIN asst ON cr.campaign = asst.campaign AND cr.day = asst.day
  LEFT JOIN campaigns c ON c.campaign_id = coalesce(cr.campaign, asst.campaign)
) TO '{os.path.join(OUT, "campaign_daily", "part-0.parquet")}' (FORMAT parquet)
""")
log("done")
