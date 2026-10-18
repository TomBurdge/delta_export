"""Randomized DuckLake workloads exported with delta_export and read back with Spark 4.1 + Delta 4.2.

FUZZ_SEEDS=<n> runs seeds 0..n-1 (default 30); FUZZ_SEED=<n> replays one seed. On failure the statement log
of the seed is printed so it can be replayed by hand.
"""

import os
import random

import duckdb
import pytest

from fuzz_model import Fuzzer
from spark_checks import assert_export_matches, assert_not_loadable, delta_versions

pytestmark = pytest.mark.spark

if os.environ.get("FUZZ_SEED"):
    SEEDS = [int(os.environ["FUZZ_SEED"])]
else:
    SEEDS = list(range(int(os.environ.get("FUZZ_SEEDS", "30"))))


def _connect(extension_path, tmp_path, seed):
    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    conn.execute("SET TimeZone = 'UTC'")
    data_path = str(tmp_path / "data")
    # odd seeds keep DuckLake's default inlining, even seeds write every insert to parquet
    limit = "" if seed % 2 else ", DATA_INLINING_ROW_LIMIT 0"
    conn.execute(
        f"INSTALL '{extension_path}'; LOAD delta_export; INSTALL ducklake;"
        f"ATTACH 'ducklake:{tmp_path / 'meta.ducklake'}' AS test_lake (DATA_PATH '{data_path}'{limit});"
        f"USE test_lake;"
    )
    return conn, data_path


class ExportChecker:
    def __init__(self, spark, data_path, seed):
        self.spark = spark
        self.data_path = data_path
        self.rng = random.Random(seed * 7 + 1)
        self.exports = 0
        self.versions = {}  # root -> versions before the current export

    def root(self, table):
        return os.path.join(self.data_path, table.schema, table.name)

    def __call__(self, fuzzer, rows):
        self.exports += 1
        label = f"seed {fuzzer.seed} export {self.exports}"
        status = {r[0]: r[1] for r in rows}
        for (schema, name), table in fuzzer.tables.items():
            key, root = f"{schema}.{name}", self.root(table)
            state = status.get(key)
            before = self.versions.get(root, [])
            if state == "needs_export":
                assert_export_matches(self.spark, fuzzer.conn, table.qualified, root, self.rng, label)
            elif state == "no_data_files":
                # an emptied table that was exported before must read as empty, not as its old rows
                if delta_versions(root):
                    df_rows = assert_export_matches(self.spark, fuzzer.conn, table.qualified, root, self.rng, label)
                    assert df_rows["rows"] == 0
            elif state == "needs_rewrite":
                assert table.risky, f"[{label}] {key}: needs_rewrite without a rename or drop + re-add"
                assert delta_versions(root) == before, f"[{label}] {key}: needs_rewrite but a new version was written"
            else:
                raise AssertionError(f"[{label}] {key}: unexpected status {state!r} in {rows}")
            self.versions[root] = delta_versions(root)
        for (schema, name), table in fuzzer.dropped.items():
            root = self.root(table)
            if self.versions.get(root):
                assert status.get(f"{schema}.{name}") == "removed_dropped", f"[{label}] {schema}.{name}: {rows}"
                assert_not_loadable(self.spark, root, label)
                self.versions[root] = []


@pytest.mark.parametrize("seed", SEEDS)
def test_fuzz_export_matches_spark(spark, extension_path, tmp_path, seed):
    conn, data_path = _connect(extension_path, tmp_path, seed)
    checker = ExportChecker(spark, data_path, seed)
    fuzzer = Fuzzer(conn, seed, on_export=checker)
    try:
        fuzzer.run()
    except Exception:
        print(f"\n===== seed {seed}: statement log =====\n{fuzzer.replay()}\n===== end seed {seed} =====")
        raise
    finally:
        conn.close()
    assert checker.exports >= 1
