"""Offline test of kvx_prices.py with saved responses. Run: python3 tests/test_collector.py"""
import json, lzma, os, shutil, sqlite3, subprocess, sys, tempfile
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FX = os.path.join(ROOT, "tests", "fixtures")
PY = [sys.executable, os.path.join(ROOT, "kvx_prices.py")]
sys.path.insert(0, ROOT)
import kvx_prices  # noqa: E402


def run(d, *args, fixtures=FX):
    env = dict(os.environ, KVX_PRICES_CONFIG=os.path.join(d, "none.json"))
    return subprocess.run(PY + ["--fixtures", fixtures, "--data-dir", d, *args], capture_output=True, text=True, env=env)


def fixture_copy():
    fx = tempfile.mkdtemp()
    shutil.copytree(FX, fx, dirs_exist_ok=True)
    return fx


d = tempfile.mkdtemp()
tmpdirs = [d]
try:
    # 1. Streaming parser: records split across tiny reads, total checked, broken input refused
    big = {"total": 2000, "dataset": "x", "records": [{"a": i, "s": "æøå " * (i % 7), "n": None} for i in range(2000)]}
    p = os.path.join(d, "big.json.xz")
    with lzma.open(p, "wt", encoding="utf-8") as fh:
        json.dump(big, fh, ensure_ascii=False)
    kvx_prices.CHUNK = 37
    got = list(kvx_prices.iter_records(p))
    assert got == big["records"], "streaming parser lost or changed records"
    with lzma.open(p, "wt", encoding="utf-8") as fh:
        fh.write(json.dumps(big)[:-500])
    try:
        list(kvx_prices.iter_records(p))
        raise AssertionError("a cut-off response was accepted")
    except ValueError:
        pass
    kvx_prices.CHUNK = 1 << 20

    # 2. First runs: baseline price list (no changes logged) and day-ahead prices
    for c in ("pricelist", "dayahead"):
        r = run(d, c)
        assert r.returncode == 0, (c, r.stdout, r.stderr)
    con = sqlite3.connect(os.path.join(d, "prices.sqlite3"))
    q = lambda sql, *a: con.execute(sql, a).fetchone()[0]
    assert q("SELECT COUNT(*) FROM runs") == 2
    assert q("SELECT COUNT(*) FROM price_rows") == 5
    assert q("SELECT COUNT(*) FROM price_changes") == 0
    assert q("SELECT COUNT(*) FROM dayahead") == 16
    assert run(d, "verify", "--full").returncode == 0

    # 3. The same response again: a new run in the chain, but no new raw file and no changes
    assert run(d, "pricelist").returncode == 0
    assert q("SELECT COUNT(DISTINCT raw_path) FROM runs WHERE source='DatahubPricelist'") == 1
    assert q("SELECT COUNT(*) FROM runs WHERE source='DatahubPricelist'") == 2
    assert q("SELECT COUNT(*) FROM price_changes") == 0

    # 4. A changed tariff, a new row, a removed row and a renamed owner are all logged
    fx = fixture_copy(); tmpdirs.append(fx)
    j = json.load(open(os.path.join(fx, "DatahubPricelist.json")))
    j["records"][3]["Price18"] = 0.45                         # new price, same key
    j["records"][4]["ChargeOwner"] = "TREFOR El-net A/S (ny)"  # renamed owner, same key
    removed = j["records"].pop(2)                              # removed
    new = dict(j["records"][1], ValidFrom="2028-01-01T00:00:00", ValidTo=None, Price1=0.9)  # announced
    j["records"].append(new)
    j["total"] = len(j["records"])
    json.dump(j, open(os.path.join(fx, "DatahubPricelist.json"), "w"), ensure_ascii=False)
    assert run(d, "pricelist", fixtures=fx).returncode == 0
    ch = dict(con.execute("SELECT change, COUNT(*) FROM price_changes GROUP BY 1").fetchall())
    assert ch == {"changed": 2, "added": 1, "removed": 1}, ch
    assert q("SELECT COUNT(*) FROM price_rows") == 5
    old = json.loads(q("SELECT old_json FROM price_changes WHERE change='removed'"))
    assert old["Note"] == removed["Note"], "the removed row's last content is kept"
    out = run(d, "changes", "--days", "3650").stdout
    assert "Price18: 0.3 → 0.45" in out and "ChargeOwner: TREFOR El-net A/S → TREFOR El-net A/S (ny)" in out, out

    # 5. A response whose 'total' disagrees with its records is refused and changes nothing
    fx = fixture_copy(); tmpdirs.append(fx)
    j = json.load(open(os.path.join(fx, "DatahubPricelist.json")))
    j["total"] = 99
    json.dump(j, open(os.path.join(fx, "DatahubPricelist.json"), "w"))
    before = q("SELECT COUNT(*) FROM price_changes")
    r = run(d, "pricelist", fixtures=fx)
    assert r.returncode == 1
    assert q("SELECT parse_error FROM runs ORDER BY id DESC LIMIT 1").startswith("the response says total=99")
    assert q("SELECT COUNT(*) FROM price_changes") == before

    # 6. A corrected day-ahead price: the first value is kept and the correction logged
    fx = fixture_copy(); tmpdirs.append(fx)
    j = json.load(open(os.path.join(fx, "DayAheadPrices.json")))
    j["records"][0]["DayAheadPriceEUR"] += 10
    json.dump(j, open(os.path.join(fx, "DayAheadPrices.json"), "w"))
    assert run(d, "dayahead", fixtures=fx).returncode == 0
    assert q("SELECT COUNT(*) FROM dayahead_revisions") == 1
    assert q("SELECT eur FROM dayahead WHERE area='DK1' AND t='2026-10-01T22:00:00Z'") == 120.5

    # 7. A failed download stays in the chain as a gap
    fx = tempfile.mkdtemp(); tmpdirs.append(fx)
    open(os.path.join(fx, "DayAheadPrices.json"), "wb").write(b"HTTP 429\nbusy")
    r = run(d, "dayahead", fixtures=fx)
    assert r.returncode == 1 and "FAILED" in r.stdout
    assert q("SELECT error FROM runs ORDER BY id DESC LIMIT 1") == "HTTP 429"
    assert run(d, "verify").returncode == 0

    # 8. Status shows the electricity tax in force and the announced one
    st = json.loads(run(d, "status", "--json").stdout)
    assert st["chain_ok"] and st["runs"] == 7, st
    assert [t["dkk_per_kwh_ex_vat"] for t in st["electricity_tax"]] == [0.008, 0.9], st["electricity_tax"]

    # 9. Changing a saved raw file breaks verification
    raw = q("SELECT raw_path FROM runs WHERE source='DayAheadPrices' AND raw_path IS NOT NULL")
    with lzma.open(os.path.join(d, raw), "wb") as fh:
        fh.write(b'{"records": []}')
    r = run(d, "verify")
    assert r.returncode == 1 and "raw file changed" in r.stdout, r.stdout

    # 10. The anchor tool reads the same chain
    out = os.path.join(d, "head.json")
    r = subprocess.run([sys.executable, os.path.join(ROOT, "tools", "anchor.py"), "snapshot", "--db", os.path.join(d, "prices.sqlite3"),
                        "--code", os.path.join(ROOT, "kvx_prices.py"), "--out", out], capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
    snap = json.load(open(out))
    assert snap["runs"] == 7 and snap["chain_ok"] == "true", snap  # the chain itself is intact; raw files are verify's job
    print("collector: streaming parser, baseline, dedup, change log, refused responses, revisions, gaps, "
          "tamper detection and anchor snapshot all check out")
finally:
    for t in tmpdirs:
        shutil.rmtree(t, ignore_errors=True)
