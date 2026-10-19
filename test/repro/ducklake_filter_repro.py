"""Find and minimise the DuckLake filter bug seen in the delta_export fuzz tests (seeds 21 and 29).

Plain DuckDB + DuckLake on the local filesystem, no delta_export and no Spark. A filtered count
(`count(*) ... WHERE pred`, which DuckLake may answer by pruning files) is compared with `count_if(pred)` over a
full scan. The workload replays the fuzz statements exactly, including what delta_export did to the catalog:
it flushes inlined rows on its own connection (same database, separate client context), and the fuzz
filters bind Python values as parameters. The statement log of the first mismatch is shrunk (ddmin) to the
smallest set that still reproduces it and printed as a standalone script.

usage: python ducklake_filter_repro.py <seed> [<seed> ...]
"""

import os
import shutil
import sys
import tempfile

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))
from fuzz_model import Fuzzer, quote  # noqa: E402

FILTERABLE = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "DOUBLE",
              "VARCHAR", "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE"}
FLUSH = "CALL ducklake_flush_inlined_data('test_lake')"
FLUSH_OTHER = FLUSH + "  -- on a second connection to the same database"


class Mismatch(Exception):
    def __init__(self, check, filtered, truth):
        super().__init__(f"{check}: filtered count {filtered}, full scan {truth}")
        self.check, self.filtered, self.truth = check, filtered, truth


def attach(work, inline):
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    conn = duckdb.connect()
    conn.execute("SET TimeZone = 'UTC'")
    limit = "" if inline else ", DATA_INLINING_ROW_LIMIT 0"
    conn.execute(f"INSTALL ducklake; ATTACH 'ducklake:{work}/meta.ducklake' AS test_lake "
                 f"(DATA_PATH '{work}/data'{limit}); USE test_lake;")
    return conn


def execute(conn, statement):
    if statement == FLUSH_OTHER:
        conn.cursor().execute(FLUSH)  # what delta_export's internal Connection does
    else:
        conn.execute(statement)


def checks_for(conn, tables):
    """(table, where, params): predicates on each column with its own values, bound as parameters (like the
    fuzz test) and as literals of the column type."""
    for table in tables:
        for name, duck_type, *_ in conn.execute(f"DESCRIBE {table}").fetchall():
            if duck_type.upper() not in FILTERABLE and not duck_type.upper().startswith("DECIMAL"):
                continue
            q = quote(name)
            values = [r[0] for r in conn.execute(
                f"SELECT DISTINCT {q} FROM {table} WHERE {q} IS NOT NULL ORDER BY 1 LIMIT 40").fetchall()]
            values += [r[0] for r in conn.execute(
                f"SELECT {q} FROM {table} WHERE {q} IS NOT NULL ORDER BY {q} DESC LIMIT 5").fetchall()]
            for v in values:
                if isinstance(v, float) and v != v:
                    continue
                yield table, f"{q} = ?", (v,)
                yield table, f"{q} > ?", (v,)
                text = conn.execute(f"SELECT ?::{duck_type}::VARCHAR", [v]).fetchone()[0]
                yield table, f"{q} = '{text.replace(chr(39), chr(39) * 2)}'::{duck_type}", ()


def counts(conn, table, where, params):
    filtered = conn.execute(f"SELECT count(*) FROM {table} WHERE {where}", list(params)).fetchone()[0]
    truth = conn.execute(f"SELECT coalesce(count_if({where}), 0) FROM {table}", list(params)).fetchone()[0]
    return filtered, truth


def run_check(conn, table, where, params):
    filtered, truth = counts(conn, table, where, params)
    if filtered < truth:  # rows the filtered query misses
        raise Mismatch((table, where, params), filtered, truth)


def find_first_mismatch(seed, work):
    conn = attach(work, inline=bool(seed % 2))

    def check(fuzzer):
        for c in checks_for(conn, [t.qualified for t in fuzzer.tables.values()]):
            run_check(conn, *c)

    fuzzer = Fuzzer(conn, seed)

    def export_step():
        fuzzer.log.append(FLUSH_OTHER)
        execute(conn, FLUSH_OTHER)
        check(fuzzer)

    fuzzer.op_export = export_step
    try:
        fuzzer.run()
    except Mismatch as m:
        refused = {i - 1 for i, s in enumerate(fuzzer.log) if s.startswith("  -- refused")}
        statements = [s for i, s in enumerate(fuzzer.log) if i not in refused and not s.startswith("  -- refused")]
        return statements, m
    finally:
        conn.close()
    return None, None


def reproduces(statements, check, seed, work):
    conn = attach(work, inline=bool(seed % 2))
    try:
        for s in statements:
            execute(conn, s)
        run_check(conn, *check)
        return False
    except Mismatch:
        return True
    except duckdb.Error:
        return False  # a needed statement was removed
    finally:
        conn.close()


def ddmin(statements, test):
    """Classic delta debugging: the smallest subsequence for which test() stays true."""
    n = 2
    while len(statements) >= 2:
        chunk = max(1, len(statements) // n)
        subsets = [statements[i:i + chunk] for i in range(0, len(statements), chunk)]
        reduced = False
        for i in range(len(subsets)):
            complement = [s for j, sub in enumerate(subsets) if j != i for s in sub]
            if test(complement):
                statements, n, reduced = complement, max(n - 1, 2), True
                break
        if not reduced:
            if n >= len(statements):
                break
            n = min(len(statements), n * 2)
    return statements


def main():
    work_root = tempfile.mkdtemp(prefix="ducklake_repro_")
    probe = duckdb.connect()
    probe.execute("INSTALL ducklake; LOAD ducklake")
    version = probe.execute(
        "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake'").fetchone()[0]
    print(f"duckdb {duckdb.__version__}, ducklake {version}")
    for seed in (int(s) for s in sys.argv[1:]):
        work = os.path.join(work_root, f"seed{seed}")
        statements, mismatch = find_first_mismatch(seed, work)
        if mismatch is None:
            print(f"\n=== seed {seed}: no mismatch")
            continue
        print(f"\n=== seed {seed}: {mismatch} after {len(statements)} statements")
        test = lambda subset: reproduces(subset, mismatch.check, seed, work)  # noqa: E731
        if not test(statements):
            print("the recorded statements do not reproduce on a fresh replay")
            continue
        minimal = ddmin(statements, test)
        conn = attach(work, inline=bool(seed % 2))
        for s in minimal:
            execute(conn, s)
        table, where, params = mismatch.check
        filtered, truth = counts(conn, table, where, params)
        conn.close()
        limit = "" if seed % 2 else ", DATA_INLINING_ROW_LIMIT 0"
        print(f"--- minimal repro for seed {seed} ({len(minimal)} statements) ---")
        print("INSTALL ducklake;")
        print(f"ATTACH 'ducklake:/tmp/repro/meta.ducklake' AS test_lake (DATA_PATH '/tmp/repro/data'{limit});")
        print("USE test_lake;")
        print("SET TimeZone = 'UTC';")
        for s in minimal:
            print(s if s == FLUSH_OTHER else s.rstrip(";") + ";")
        bound = f"  -- ? = {params[0]!r} ({type(params[0]).__name__})" if params else ""
        print(f"SELECT count(*) FROM {table} WHERE {where};  -- returns {filtered}{bound}")
        print(f"SELECT count_if({where}) FROM {table};  -- returns {truth}")
        print("--- end ---")


if __name__ == "__main__":
    main()
