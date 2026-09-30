#!/usr/bin/env python3
"""kvx_prices.py: Kvantix energy price archive collector (see METHOD.md).

Saves, as published, the full DataHub price list (every grid tariff, Energinet tariff, fee and the
electricity tax) once a day, and the day-ahead electricity prices for DK1 and DK2 once a day. Every
download is saved compressed and locked in a SHA-256 hash chain BEFORE it is read, like the weather
test and the energy test. Standard library only; touches nothing else on the server.

    kvx_prices.py probe              small request to each dataset, shows what was read (no saving)
    kvx_prices.py pricelist          full DatahubPricelist                      (timer: daily 05:17 UTC)
    kvx_prices.py dayahead           day-ahead prices, yesterday to tomorrow    (timer: daily 13:47 UTC)
    kvx_prices.py verify [--full]    recompute the hash chain and check every raw file
    kvx_prices.py status [--json]    counts, last downloads, chain state, current electricity tax
    kvx_prices.py changes [--days N] price list changes seen in the last N days (default 7)

Source: Energinet (www.energidataservice.dk), CC BY 4.0. No API key; no personal data.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import lzma
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

VERSION = "1.0"
UTC = timezone.utc
API = "https://api.energidataservice.dk/dataset/"
CONFIG_PATH = os.environ.get("KVX_PRICES_CONFIG", "/etc/kvx-prices/config.json")
DEFAULT_CONFIG = {
    "data_dir": "/var/lib/kvx-prices",
    "user_agent": "kvantix-prices/1.0 (https://kvantix.tech; validation@kvantix.tech)",
}
AREAS = ("DK1", "DK2")
FIXTURES = None  # set by --fixtures (offline tests): folder with saved responses named <dataset>.json
CHUNK = 1 << 20  # bytes per read when streaming (tests make this small)

# What each command downloads: (kind, dataset, query). "now" is Energi Data Service's dynamic time.
JOBS = {
    "pricelist": [("published", "DatahubPricelist", {"limit": 0})],
    "dayahead": [("published", "DayAheadPrices", {"start": "now-P1D", "end": "now+P2D",
                                                  "filter": json.dumps({"PriceArea": list(AREAS)}, separators=(",", ":")),
                                                  "limit": 0})],
}
# A price list row is identified by who charges it (GLN number), the charge and the date it applies from.
# A renamed owner or a new price for the same key is logged as "changed", not as a removal plus an addition.
KEY_FIELDS = ("GLN_Number", "ChargeType", "ChargeTypeCode", "ValidFrom")


# ------------------------------------------------------------ util

def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, encoding="utf-8") as fh:
            cfg.update(json.load(fh))
    return cfg


def url_for(dataset: str, query: dict) -> str:
    return API + dataset + "?" + urllib.parse.urlencode(query)


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 16), b""):
            h.update(block)
    return h.hexdigest()


def download(url: str, cfg: dict, dataset: str, tmp_path: str, waits=(20, 60, 120)) -> tuple[int, str | None, int]:
    """Streams the response into an xz file at tmp_path while hashing it. Returns (http status, sha256 of the
    uncompressed payload or None, payload bytes). On 429/5xx waits (Retry-After if given) and retries."""
    def save(stream) -> tuple[str, int]:
        h, n = hashlib.sha256(), 0
        with lzma.open(tmp_path, "wb", preset=6) as out:
            for block in iter(lambda: stream.read(CHUNK), b""):
                h.update(block)
                out.write(block)
                n += len(block)
        return h.hexdigest(), n

    if FIXTURES is not None:
        path = os.path.join(FIXTURES, dataset + ".json")
        if not os.path.exists(path):
            return 404, None, 0
        with open(path, "rb") as fh:
            if fh.read(5) == b"HTTP ":
                fh.seek(5)
                return int(fh.read(3)), None, 0
            fh.seek(0)
            sha, n = save(fh)
        return 200, sha, n
    req = urllib.request.Request(url, headers={"User-Agent": cfg["user_agent"], "Accept": "application/json"})
    status = 0
    for attempt in range(len(waits) + 1):
        retry_after = None
        try:
            with urllib.request.urlopen(req, timeout=300) as r:
                sha, n = save(r)
                return r.status, sha, n
        except urllib.error.HTTPError as e:
            status = e.code
            ra = e.headers.get("Retry-After") if e.headers else None
            retry_after = int(ra) if ra and ra.strip().isdigit() else None
            if e.code not in (429, 500, 502, 503, 504):
                return status, None, 0
        except (urllib.error.URLError, TimeoutError, OSError):
            status = 0
        if attempt < len(waits):
            time.sleep(min(300, retry_after if retry_after is not None else waits[attempt]))
    return status, None, 0


# ------------------------------------------------------------ database + hash chain (same rules as the weather test)

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  id INTEGER PRIMARY KEY,
  kind TEXT NOT NULL,              -- published
  source TEXT NOT NULL,            -- dataset name
  location TEXT NOT NULL,          -- 'DK'
  fetched_at TEXT NOT NULL,
  http_status INTEGER,
  url TEXT,
  payload_sha256 TEXT,             -- SHA-256 of the response exactly as received (uncompressed)
  raw_path TEXT,                   -- raw/<dataset>/<sha[:2]>/<sha>.json.xz; identical responses share one file
  file_sha256 TEXT,                -- SHA-256 of the .xz file (fast check); not part of the chain
  payload_bytes INTEGER,
  n_rows INTEGER DEFAULT 0,
  error TEXT,
  parse_error TEXT,
  prev_hash TEXT NOT NULL,
  chain_hash TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS price_rows (           -- the price list as last seen, one row per key
  key TEXT PRIMARY KEY, row_sha256 TEXT NOT NULL, json TEXT NOT NULL,
  first_seen_run INTEGER NOT NULL, last_seen_run INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS price_changes (        -- every addition, change and removal after the first snapshot
  run_id INTEGER NOT NULL, seen_at TEXT NOT NULL, change TEXT NOT NULL, key TEXT NOT NULL,
  old_json TEXT, new_json TEXT
);
CREATE INDEX IF NOT EXISTS price_changes_run ON price_changes(run_id);
CREATE TABLE IF NOT EXISTS dayahead (             -- first published value is kept
  area TEXT NOT NULL, t TEXT NOT NULL, eur REAL, dkk REAL, run_id INTEGER NOT NULL,
  PRIMARY KEY (area, t)
);
CREATE TABLE IF NOT EXISTS dayahead_revisions (
  area TEXT, t TEXT, old_eur REAL, new_eur REAL, old_dkk REAL, new_dkk REAL, run_id INTEGER, seen_at TEXT
);
"""
GENESIS = "0" * 64
CHAIN_FIELDS = ("kind", "source", "location", "fetched_at", "http_status", "url", "payload_sha256", "n_rows", "error")


def db_connect(cfg) -> sqlite3.Connection:
    os.makedirs(cfg["data_dir"], exist_ok=True)
    con = sqlite3.connect(os.path.join(cfg["data_dir"], "prices.sqlite3"))
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    return con


def chain_hash(prev: str, fields: dict) -> str:
    rec = json.dumps(fields, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256((prev + "|" + rec).encode()).hexdigest()


def add_run(con, cfg, *, kind, source, fetched_at, status, url, sha, nbytes, tmp_path, error=None) -> int:
    """Moves the raw response into place and adds it to the hash chain BEFORE anything is read from it."""
    raw_path = file_sha = None
    if sha:
        rel = os.path.join("raw", source, sha[:2], sha + ".json.xz")
        full = os.path.join(cfg["data_dir"], rel)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        if os.path.exists(full):
            os.remove(tmp_path)  # same response as before: one file is enough, the chain still records this run
        else:
            os.replace(tmp_path, full)
        raw_path, file_sha = rel, sha256_file(full)
    elif tmp_path and os.path.exists(tmp_path):
        os.remove(tmp_path)
    prev = con.execute("SELECT chain_hash FROM runs ORDER BY id DESC LIMIT 1").fetchone()
    prev = prev[0] if prev else GENESIS
    fields = {"kind": kind, "source": source, "location": "DK", "fetched_at": fetched_at, "http_status": status,
              "url": url, "payload_sha256": sha, "n_rows": 0, "error": error}
    h = chain_hash(prev, fields)
    cur = con.execute(
        "INSERT INTO runs (kind, source, location, fetched_at, http_status, url, payload_sha256, raw_path, file_sha256,"
        " payload_bytes, n_rows, error, prev_hash, chain_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (kind, source, "DK", fetched_at, status, url, sha, raw_path, file_sha, nbytes, 0, error, prev, h))
    with open(os.path.join(cfg["data_dir"], "chain.log"), "a", encoding="utf-8") as fh:
        fh.write(f"{cur.lastrowid}\t{fetched_at}\t{kind}\t{source}\t{sha or '-'}\t{h}\n")
    return cur.lastrowid


def verify_chain(con, cfg, full=False) -> tuple[bool, list[str]]:
    """The chain always; every raw file by its .xz hash; with full=True also decompressed against payload_sha256."""
    problems, prev, checked = [], GENESIS, {}
    for r in con.execute(f"SELECT id, prev_hash, chain_hash, raw_path, file_sha256, {', '.join(CHAIN_FIELDS)} FROM runs ORDER BY id"):
        rid, p, h, raw_path, file_sha = r[0], r[1], r[2], r[3], r[4]
        fields = dict(zip(CHAIN_FIELDS, r[5:]))
        fields["n_rows"] = 0
        if p != prev:
            problems.append(f"run {rid}: prev_hash does not match")
        if chain_hash(p, fields) != h:
            problems.append(f"run {rid}: chain_hash does not match")
        if raw_path:
            full_path = os.path.join(cfg["data_dir"], raw_path)
            if raw_path not in checked:
                try:
                    ok = sha256_file(full_path) == file_sha
                    if ok and full:
                        hh = hashlib.sha256()
                        with lzma.open(full_path, "rb") as fh:
                            for block in iter(lambda: fh.read(1 << 20), b""):
                                hh.update(block)
                        ok = hh.hexdigest() == fields["payload_sha256"]
                    if ok and not os.path.basename(raw_path).startswith(fields["payload_sha256"] or "-"):
                        ok = False
                    checked[raw_path] = "ok" if ok else "changed"
                except OSError:
                    checked[raw_path] = "missing"
            if checked[raw_path] != "ok":
                problems.append(f"run {rid}: raw file {checked[raw_path]} ({raw_path})")
        prev = h
    return (not problems), problems


# ------------------------------------------------------------ parsing (streaming: the price list is ~100 MB of JSON)

def iter_records(path: str):
    """Yields each record of an Energi Data Service response ({..., "records": [ {...}, ... ]}) without loading
    the whole file. Returns the 'total' field (if it came before the records) as the generator's return value."""
    dec = json.JSONDecoder()
    total = None
    with lzma.open(path, "rt", encoding="utf-8") as fh:
        buf, pos, started, eof = "", 0, False, False
        while True:
            if not eof:
                chunk = fh.read(CHUNK)
                eof = chunk == ""
                buf = buf[pos:] + chunk
                pos = 0
            if not started:
                i = buf.find('"records"')
                j = buf.find("[", i) if i >= 0 else -1
                if j < 0:
                    if eof:
                        raise ValueError("no 'records' list in the response")
                    continue
                m = re.search(r'"total"\s*:\s*(\d+)', buf[:i])
                total = int(m.group(1)) if m else None
                pos, started = j + 1, True
            while True:
                while pos < len(buf) and buf[pos] in " \t\r\n,":
                    pos += 1
                if pos < len(buf) and buf[pos] == "]":
                    return total
                if pos >= len(buf):
                    break
                try:
                    obj, end = dec.raw_decode(buf, pos)
                except json.JSONDecodeError:
                    if eof:
                        raise ValueError("response ends inside a record")
                    break
                if not isinstance(obj, dict):
                    raise ValueError("a record is not an object")
                yield obj
                pos = end
            if eof:
                raise ValueError("response ends before the records list is closed")


def utc_key(s: str) -> str:
    return s[:16] + ":00Z" if len(s) >= 16 else s


def store_pricelist(con, run_id, path, fetched_at) -> int:
    known = dict(con.execute("SELECT key, row_sha256 FROM price_rows"))
    baseline = not known
    seen = set()
    n = 0
    gen = iter_records(path)
    total = None
    while True:
        try:
            r = next(gen)
        except StopIteration as stop:
            total = stop.value
            break
        n += 1
        key = json.dumps([r.get(k) for k in KEY_FIELDS], ensure_ascii=False, separators=(",", ":"))
        k, i = key, 1
        while k in seen:  # a key that repeats inside one snapshot gets a suffix, so no row is lost
            i += 1
            k = f"{key}#{i}"
        seen.add(k)
        js = json.dumps(r, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        sha = hashlib.sha256(js.encode()).hexdigest()
        old = known.get(k)
        if old is None:
            con.execute("INSERT INTO price_rows VALUES (?,?,?,?,?)", (k, sha, js, run_id, run_id))
            if not baseline:
                con.execute("INSERT INTO price_changes VALUES (?,?,?,?,?,?)", (run_id, fetched_at, "added", k, None, js))
        elif old != sha:
            old_js = con.execute("SELECT json FROM price_rows WHERE key=?", (k,)).fetchone()[0]
            con.execute("UPDATE price_rows SET row_sha256=?, json=?, last_seen_run=? WHERE key=?", (sha, js, run_id, k))
            con.execute("INSERT INTO price_changes VALUES (?,?,?,?,?,?)", (run_id, fetched_at, "changed", k, old_js, js))
        else:
            con.execute("UPDATE price_rows SET last_seen_run=? WHERE key=?", (run_id, k))
    if total is not None and total != n:
        raise ValueError(f"the response says total={total} but contains {n} records")
    if n == 0:
        raise ValueError("empty price list; previous rows kept, nothing marked as removed")
    for k in set(known) - seen:
        old_js = con.execute("SELECT json FROM price_rows WHERE key=?", (k,)).fetchone()[0]
        con.execute("INSERT INTO price_changes VALUES (?,?,?,?,?,?)", (run_id, fetched_at, "removed", k, old_js, None))
        con.execute("DELETE FROM price_rows WHERE key=?", (k,))
    return n


def store_dayahead(con, run_id, path, fetched_at) -> int:
    f = lambda x: None if x is None else float(x)
    n = 0
    for r in iter_records(path):
        if r.get("PriceArea") not in AREAS or not r.get("TimeUTC"):
            continue
        area, t, eur, dkk = r["PriceArea"], utc_key(r["TimeUTC"]), f(r.get("DayAheadPriceEUR")), f(r.get("DayAheadPriceDKK"))
        old = con.execute("SELECT eur, dkk FROM dayahead WHERE area=? AND t=?", (area, t)).fetchone()
        if old is None:
            con.execute("INSERT INTO dayahead VALUES (?,?,?,?,?)", (area, t, eur, dkk, run_id))
        elif old[0] != eur or old[1] != dkk:  # first value is kept; the correction is logged
            con.execute("INSERT INTO dayahead_revisions VALUES (?,?,?,?,?,?,?,?)", (area, t, old[0], eur, old[1], dkk, run_id, fetched_at))
        n += 1
    return n


STORE = {"DatahubPricelist": store_pricelist, "DayAheadPrices": store_dayahead}


# ------------------------------------------------------------ commands

def run_job(con, cfg, job, quiet=False) -> int:
    failures = 0
    tmp_dir = os.path.join(cfg["data_dir"], "tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    for kind, dataset, query in JOBS[job]:
        url = url_for(dataset, query)
        fetched_at = iso(now_utc())
        tmp = os.path.join(tmp_dir, f"{dataset}.{os.getpid()}.json.xz")
        status, sha, nbytes = download(url, cfg, dataset, tmp)
        ok = status == 200 and sha and nbytes > 0
        with con:
            rid = add_run(con, cfg, kind=kind, source=dataset, fetched_at=fetched_at, status=status, url=url,
                          sha=sha if ok else None, nbytes=nbytes if ok else 0, tmp_path=tmp,
                          error=None if ok else f"HTTP {status}")
        if not ok:
            failures += 1
            if not quiet:
                print(f"{dataset:17} FAILED http {status} (kept in the chain as a gap, run {rid})")
            continue
        raw = con.execute("SELECT raw_path FROM runs WHERE id=?", (rid,)).fetchone()[0]
        try:
            with con:
                n = STORE[dataset](con, rid, os.path.join(cfg["data_dir"], raw), fetched_at)
                con.execute("UPDATE runs SET n_rows=? WHERE id=?", (n, rid))
        except (ValueError, KeyError, TypeError) as e:
            with con:
                con.execute("UPDATE runs SET parse_error=? WHERE id=?", (str(e)[:300], rid))
            failures += 1
            n = 0
        if not quiet:
            print(f"{dataset:17} http {status}  {nbytes:>10} bytes  {n:>7} rows  run {rid}")
    return 1 if failures else 0


def current_tax(con):
    """The electricity tax (Note 'Elafgift') valid now and any announced change, from the stored price list."""
    now = iso(now_utc())[:19]
    out = []
    for (js,) in con.execute("SELECT json FROM price_rows WHERE json LIKE '%\"Note\":\"Elafgift\"%'"):
        r = json.loads(js)
        if (r.get("ValidTo") or "9999") > now:
            out.append({"valid_from": r.get("ValidFrom"), "valid_to": r.get("ValidTo"), "dkk_per_kwh_ex_vat": r.get("Price1")})
    return sorted(out, key=lambda x: x["valid_from"] or "")


def status(con, cfg, as_json=False) -> int:
    ok, problems = verify_chain(con, cfg)
    last = con.execute("SELECT id, fetched_at, chain_hash FROM runs ORDER BY id DESC LIMIT 1").fetchone()
    by_source = {s: {"runs": n, "failed": f, "last": t} for s, n, f, t in con.execute(
        "SELECT source, COUNT(*), SUM(error IS NOT NULL OR parse_error IS NOT NULL), MAX(fetched_at) FROM runs GROUP BY 1")}
    since = iso(now_utc() - timedelta(days=30))
    out = {"version": VERSION, "chain_ok": ok, "runs": con.execute("SELECT COUNT(*) FROM runs").fetchone()[0],
           "last_run_id": last[0] if last else None, "last_fetched_at": last[1] if last else None,
           "chain_head": last[2] if last else GENESIS, "by_source": by_source,
           "price_rows": con.execute("SELECT COUNT(*) FROM price_rows").fetchone()[0],
           "price_changes_30d": dict(con.execute("SELECT change, COUNT(*) FROM price_changes WHERE seen_at >= ? GROUP BY 1", (since,))),
           "electricity_tax": current_tax(con),
           "dayahead_values": con.execute("SELECT COUNT(*) FROM dayahead").fetchone()[0],
           "dayahead_revisions": con.execute("SELECT COUNT(*) FROM dayahead_revisions").fetchone()[0],
           "raw_files": len({r[0] for r in con.execute("SELECT raw_path FROM runs WHERE raw_path IS NOT NULL")}),
           "problems": problems[:20]}
    if as_json:
        print(json.dumps(out, indent=1, ensure_ascii=False))
    else:
        print(f"kvx_prices {VERSION} · runs {out['runs']} · chain {'intact' if ok else 'BROKEN'} · last {out['last_fetched_at']}")
        for s, v in sorted(by_source.items()):
            print(f"  {s:20} {v['runs']:>5} runs  {v['failed'] or 0:>3} failed  last {v['last']}")
        print(f"  price list rows {out['price_rows']}, changes last 30 days {out['price_changes_30d'] or 0}")
        for t in out["electricity_tax"]:
            print(f"  electricity tax {t['dkk_per_kwh_ex_vat']} DKK/kWh ex VAT, {t['valid_from']} to {t['valid_to']}")
        print(f"  day-ahead values {out['dayahead_values']}, revisions {out['dayahead_revisions']}, raw files {out['raw_files']}")
        for p in problems[:20]:
            print("  PROBLEM:", p)
    return 0 if ok else 1


def changes(con, days) -> int:
    since = iso(now_utc() - timedelta(days=days))
    rows = con.execute("SELECT seen_at, change, old_json, new_json FROM price_changes WHERE seen_at >= ? ORDER BY rowid", (since,)).fetchall()
    for seen_at, change, old, new in rows:
        r = json.loads(new or old)
        extra = ""
        if change == "changed":
            o, nw = json.loads(old), json.loads(new)
            diff = [k for k in sorted(set(o) | set(nw)) if o.get(k) != nw.get(k)]
            extra = " · " + ", ".join(f"{k}: {o.get(k)} → {nw.get(k)}" for k in diff[:4]) + (" …" if len(diff) > 4 else "")
        print(f"{seen_at}  {change:8} {r.get('ChargeOwner') or r.get('GLN_Number')} · {r.get('Note')} · from {r.get('ValidFrom')}{extra}")
    print(f"{len(rows)} change(s) since {since}")
    return 0


def probe(cfg) -> int:
    rc = 0
    tests = [("DatahubPricelist", {"limit": 3, "sort": "ValidFrom desc"}), ("DayAheadPrices", JOBS["dayahead"][0][2])]
    for dataset, q in tests:
        req = urllib.request.Request(url_for(dataset, q), headers={"User-Agent": cfg["user_agent"]})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                j = json.load(r)
            recs = j.get("records", [])
            print(f"{dataset:17} http 200  {len(recs):>5} rows (total {j.get('total')})  first: {json.dumps(recs[0] if recs else None, ensure_ascii=False)[:150]}")
        except Exception as e:  # noqa: BLE001 - shown to the operator, nothing saved
            print(f"{dataset:17} FAILED {e}")
            rc = 1
    return rc


def main(argv=None) -> int:
    global FIXTURES
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--fixtures", help="offline test: read saved responses from this folder")
    ap.add_argument("--data-dir", help="override data_dir (tests)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for c in ("probe", "pricelist", "dayahead"):
        sub.add_parser(c)
    v = sub.add_parser("verify")
    v.add_argument("--full", action="store_true", help="also decompress every raw file and check its payload hash")
    s = sub.add_parser("status")
    s.add_argument("--json", action="store_true")
    c = sub.add_parser("changes")
    c.add_argument("--days", type=int, default=7)
    a = ap.parse_args(argv)
    FIXTURES = a.fixtures
    cfg = load_config()
    if a.data_dir:
        cfg["data_dir"] = a.data_dir
    if a.cmd == "probe":
        return probe(cfg)
    con = db_connect(cfg)
    if a.cmd in JOBS:
        return run_job(con, cfg, a.cmd)
    if a.cmd == "verify":
        ok, problems = verify_chain(con, cfg, full=a.full)
        print("chain intact" if ok else "CHAIN BROKEN:\n  " + "\n  ".join(problems))
        return 0 if ok else 1
    if a.cmd == "status":
        return status(con, cfg, a.json)
    if a.cmd == "changes":
        return changes(con, a.days)
    return 2


if __name__ == "__main__":
    sys.exit(main())
