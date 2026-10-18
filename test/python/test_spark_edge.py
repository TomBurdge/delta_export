"""Targeted edge cases for delta_export, read back with Spark 4.1 + Delta 4.2 (Fabric Runtime 2.0)."""

import os
import random

import duckdb
import pytest

from spark_checks import assert_export_matches, delta_versions, load

pytestmark = pytest.mark.spark


@pytest.fixture
def lake(extension_path, tmp_path):
    """(conn, data_path) with every insert written straight to parquet."""
    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    conn.execute("SET TimeZone = 'UTC'")
    data_path = str(tmp_path / "data")
    conn.execute(
        f"INSTALL '{extension_path}'; LOAD delta_export; INSTALL ducklake;"
        f"ATTACH 'ducklake:{tmp_path / 'meta.ducklake'}' AS test_lake (DATA_PATH '{data_path}', DATA_INLINING_ROW_LIMIT 0);"
        f"USE test_lake;"
    )
    yield conn, data_path
    conn.close()


def _export(conn):
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    return {r[0]: r for r in conn.execute("CALL delta_export()").fetchall()}


def _root(data_path, table, schema="main"):
    return os.path.join(data_path, schema, table)


def _check(spark, conn, data_path, table):
    return assert_export_matches(spark, conn, table, _root(data_path, table), random.Random(table))


# ── deletion vectors ──


def test_big_deletion_vector_bitmap_containers(spark, lake):
    """300k rows in one file: every 2nd row plus a 70k range deleted -> roaring bitmap containers, several keys."""
    conn, data_path = lake
    conn.execute("CREATE TABLE big AS SELECT i AS id, i * 3 AS v FROM range(300000) t(i)")
    conn.execute("DELETE FROM big WHERE id % 2 = 0 OR id BETWEEN 100000 AND 170000")
    _export(conn)
    info = _check(spark, conn, data_path, "big")
    assert info["dv"] and info["files"] == 1


def test_fully_deleted_file_and_many_small_files(spark, lake):
    conn, data_path = lake
    conn.execute("CREATE TABLE small (id BIGINT, v VARCHAR)")
    for batch in range(500):
        conn.execute(f"INSERT INTO small SELECT {batch * 10} + i, 'b{batch}' FROM range(10) t(i)")
    conn.execute("DELETE FROM small WHERE (id // 10) % 2 = 0 AND id % 3 = 0")
    conn.execute("DELETE FROM small WHERE id < 10")  # the first file loses every row
    _export(conn)
    info = _check(spark, conn, data_path, "small")
    assert info["dv"] and info["files"] >= 400


# ── statistics traps: a wrong bound makes data skipping drop rows ──


def test_stats_traps(spark, lake):
    conn, data_path = lake
    conn.execute(
        "CREATE TABLE traps (id BIGINT, s VARCHAR, ts TIMESTAMP, tstz TIMESTAMPTZ, f DOUBLE, only_nan DOUBLE, "
        "only_null INTEGER, d DECIMAL(38,10), dt DATE)"
    )
    conn.execute(
        "INSERT INTO traps SELECT i, repeat('p', 64) || lpad(i::VARCHAR, 6, '0'), "
        "TIMESTAMP '2024-01-01 00:00:00.000001' + to_microseconds(i), "
        "TIMESTAMPTZ '1969-12-31 23:59:59.999999+00' + to_microseconds(i * 7), "
        "CASE WHEN i % 10 = 0 THEN 'nan'::DOUBLE ELSE i / 3 END, 'nan'::DOUBLE, NULL, "
        "(i * 1.0000000001)::DECIMAL(38,10), DATE '0001-01-01' + i::INTEGER "
        "FROM range(5000) t(i)"
    )
    conn.execute("INSERT INTO traps VALUES (99999, NULL, NULL, NULL, 'inf', NULL, NULL, NULL, DATE '9999-12-31')")
    _export(conn)
    _check(spark, conn, data_path, "traps")
    df = load(spark, _root(data_path, "traps"))
    # boundary probes on the micro-second and long-prefix columns
    for sql_filter, spark_filter in [
        ("ts = TIMESTAMP '2024-01-01 00:00:00.004999'", "ts = TIMESTAMP '2024-01-01 00:00:00.004999'"),
        ("ts > TIMESTAMP '2024-01-01 00:00:00.004999'", "ts > TIMESTAMP '2024-01-01 00:00:00.004999'"),
        ("s = repeat('p', 64) || '004999'", "s = concat(repeat('p', 64), '004999')"),
        ("s > repeat('p', 64) || '004998'", "s > concat(repeat('p', 64), '004998')"),
        ("tstz < TIMESTAMPTZ '1970-01-01 00:00:00+00'", "tstz < TIMESTAMP '1970-01-01 00:00:00'"),
        ("dt = DATE '9999-12-31'", "dt = DATE '9999-12-31'"),
    ]:
        want = conn.execute(f"SELECT count(*) FROM traps WHERE {sql_filter}").fetchone()[0]
        assert df.filter(spark_filter).count() == want, sql_filter


def test_tables_with_overlapping_column_ids(spark, lake):
    conn, data_path = lake
    conn.execute("CREATE TABLE a AS SELECT i AS id, i AS x FROM range(1000) t(i)")
    conn.execute("CREATE TABLE b AS SELECT i + 1000000 AS id, -i AS x FROM range(1000) t(i)")
    _export(conn)
    _check(spark, conn, data_path, "a")
    _check(spark, conn, data_path, "b")


# ── schema evolution ──


def test_type_widening_chain(spark, lake):
    """Each step leaves files of the narrower type; Spark reads them through the typeWidening feature."""
    conn, data_path = lake
    conn.execute("CREATE TABLE w (id BIGINT, n TINYINT, f FLOAT, u UTINYINT)")
    steps = [None, ("n", "SMALLINT"), ("n", "INTEGER"), ("n", "BIGINT"), ("f", "DOUBLE"), ("u", "USMALLINT"),
             ("u", "UINTEGER")]
    for step, change in enumerate(steps):
        if change:
            conn.execute(f"ALTER TABLE w ALTER COLUMN {change[0]} TYPE {change[1]}")
        conn.execute(f"INSERT INTO w SELECT {step * 1000} + i, (i % 100) - 50, i / 7, i % 200 FROM range(1000) t(i)")
    conn.execute("INSERT INTO w VALUES (99999, 9223372036854775807, 1e300, 4294967295)")
    _export(conn)
    info = _check(spark, conn, data_path, "w")
    assert info["widening"]


def test_rename_needs_rewrite_then_rewrite_exports(spark, lake):
    """Old files keep the old name; name-based readers (Fabric SQL endpoint) would read NULL."""
    conn, data_path = lake
    conn.execute("CREATE TABLE r AS SELECT i AS id, 'n' || i AS name FROM range(100) t(i)")
    _export(conn)
    before = delta_versions(_root(data_path, "r"))
    conn.execute("ALTER TABLE r RENAME COLUMN name TO full_name")
    conn.execute("INSERT INTO r VALUES (1000, 'new')")
    rows = _export(conn)
    assert rows["main.r"][1] == "needs_rewrite", rows
    assert "name" in rows["main.r"][4], rows  # the message names the column
    assert delta_versions(_root(data_path, "r")) == before

    conn.execute("CREATE OR REPLACE TABLE r AS SELECT * FROM r")
    rows = _export(conn)
    assert rows["main.r"][1] == "needs_export", rows
    _check(spark, conn, data_path, "r")


def test_drop_and_readd_same_name_needs_rewrite(spark, lake):
    conn, data_path = lake
    conn.execute("CREATE TABLE d AS SELECT i AS id, i * 2 AS v FROM range(100) t(i)")
    conn.execute("ALTER TABLE d DROP COLUMN v")
    conn.execute("ALTER TABLE d ADD COLUMN v VARCHAR")
    conn.execute("INSERT INTO d VALUES (1000, 'x')")
    rows = _export(conn)
    assert rows["main.d"][1] == "needs_rewrite", rows


def test_drop_column_keeps_reading(spark, lake):
    conn, data_path = lake
    conn.execute("CREATE TABLE dc AS SELECT i AS id, i * 2 AS v, 'x' AS gone FROM range(100) t(i)")
    conn.execute("ALTER TABLE dc DROP COLUMN gone")
    conn.execute("ALTER TABLE dc ADD COLUMN extra INTEGER")
    conn.execute("INSERT INTO dc VALUES (1000, 1, 7)")
    rows = _export(conn)
    assert rows["main.dc"][1] == "needs_export", rows
    _check(spark, conn, data_path, "dc")


@pytest.mark.parametrize("column", ["with space", "with.dot", "ünïcødé", "MixedCase", "a,b", "x=y"])
def test_special_column_names(spark, lake, column):
    conn, data_path = lake
    quoted = '"' + column + '"'
    conn.execute(f"CREATE TABLE sc (id BIGINT, {quoted} INTEGER)")
    conn.execute(f"INSERT INTO sc SELECT i, i FROM range(50) t(i)")
    rows = _export(conn)
    status = rows["main.sc"][1]
    if any(ch in column for ch in " ,;{}()\n\t="):
        assert status == "unsupported_column_name", rows
        assert not delta_versions(_root(data_path, "sc"))
    else:
        assert status == "needs_export", rows
        _check(spark, conn, data_path, "sc")


# ── table lifecycle ──


def test_emptied_table_reads_empty(spark, lake):
    conn, data_path = lake
    conn.execute("CREATE TABLE e AS SELECT i AS id FROM range(100) t(i)")
    _export(conn)
    conn.execute("DELETE FROM e")
    conn.execute("CALL test_lake.set_option('rewrite_delete_threshold', 0)")
    conn.execute("CALL ducklake_rewrite_data_files('test_lake')")
    _export(conn)
    assert load(spark, _root(data_path, "e")).count() == 0


def test_repeated_exports_and_history(spark, lake):
    conn, data_path = lake
    conn.execute("CREATE TABLE h AS SELECT i AS id FROM range(10) t(i)")
    _export(conn)
    root = _root(data_path, "h")
    first = delta_versions(root)
    for _ in range(10):
        _export(conn)
    assert delta_versions(root) == first, "re-export without a change must not add versions"

    last = max(first)
    for step in range(10):
        conn.execute(f"INSERT INTO h VALUES ({100 + step})")
        if step % 3 == 0:
            conn.execute(f"DELETE FROM h WHERE id = {step}")
        _export(conn)
        now = max(delta_versions(root))
        assert now > last
        last = now
        _check(spark, conn, data_path, "h")
    history = spark.sql(f"DESCRIBE HISTORY delta.`{root}`").collect()
    assert history and history[0].version == last
