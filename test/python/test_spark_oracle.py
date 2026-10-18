"""Spark + Delta Lake is the reference Delta reader: every export must read back identically in Spark.

Runs in the spark-tests CI job (pytest -m spark). Columns delta_export maps to a lossy 'string'
(uint64, hugeint, uuid, time, interval) are deliberately left out: Delta has no type for them.
"""

import os

import duckdb
import pytest

from delta_readers import find_delta_roots

pytestmark = pytest.mark.spark


def _export(conn, flush=True):
    if flush:
        conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    return conn.execute("CALL delta_export()").fetchall()


def _spark_rows(spark, root, columns, order_by):
    # DeltaLog instances are cached per path; a re-export of the same folder must be re-read
    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    df = spark.read.format("delta").load(root)
    return [tuple(row) for row in df.select(*columns).orderBy(order_by).collect()]


def _assert_spark_matches(spark, conn, table, root, columns, order_by):
    expected = conn.execute(f"SELECT {', '.join(columns)} FROM {table} ORDER BY {order_by}").fetchall()
    actual = _spark_rows(spark, root, columns, order_by)
    assert len(actual) == len(expected), f"Spark read {len(actual)} rows, DuckLake has {len(expected)}"
    assert actual == expected


def test_spark_primitive_types_multi_table(spark, ducklake_env):
    conn, data_path = ducklake_env
    conn.execute(
        "CREATE TABLE typed AS SELECT i::TINYINT AS i8, i::SMALLINT AS i16, i::INTEGER AS i32, i::BIGINT AS i64, "
        "i::UTINYINT AS u8, i::USMALLINT AS u16, i::UINTEGER AS u32, "
        "(i / 3)::FLOAT AS f32, (i / 3)::DOUBLE AS f64, (i * 1.25)::DECIMAL(10, 2) AS dec, "
        "i % 2 = 0 AS flag, 'row ' || i AS label, DATE '2024-01-01' + i::INTEGER AS d, "
        "TIMESTAMP '2024-01-01 10:00:00' + to_seconds(i) AS ts, "
        "TIMESTAMPTZ '2024-01-01 10:00:00+00' + to_seconds(i) AS tstz, "
        "('blob' || i)::BLOB AS b "
        "FROM range(100) t(i)"
    )
    conn.execute("CREATE TABLE other AS SELECT i AS id FROM range(7) t(i)")
    _export(conn)
    roots = find_delta_roots(data_path)

    columns = ["i8", "i16", "i32", "i64", "u8", "u16", "u32", "f32", "f64", "dec", "flag", "label", "d", "b"]
    expected = conn.execute(f"SELECT {', '.join(columns)} FROM typed ORDER BY i64").fetchall()
    actual = _spark_rows(spark, roots["typed"], columns, "i64")
    assert [tuple(bytes(v) if isinstance(v, (bytearray, memoryview)) else v for v in r) for r in actual] == expected

    # timestamps: compare as UTC instants
    ts = conn.execute(
        "SELECT ts, timezone('UTC', tstz) FROM typed ORDER BY i64"
    ).fetchall()
    spark_ts = _spark_rows(spark, roots["typed"], ["ts", "tstz"], "i64")
    assert spark_ts == ts

    _assert_spark_matches(spark, conn, "other", roots["other"], ["id"], "id")


def test_spark_deletes_update_merge(spark, ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE events AS SELECT i AS id, i % 10 AS grp, i * 1.5 AS value FROM range(5000) t(i)")
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    conn.execute("DELETE FROM events WHERE id % 7 = 0")
    conn.execute("UPDATE events SET value = -1 WHERE id % 11 = 0")
    conn.execute(
        "MERGE INTO events USING (SELECT i AS id FROM range(4990, 5010) t(i)) src ON events.id = src.id "
        "WHEN MATCHED THEN UPDATE SET value = 0 "
        "WHEN NOT MATCHED THEN INSERT VALUES (src.id, -1, 0)"
    )
    _export(conn)
    root = find_delta_roots(data_path)["events"]
    _assert_spark_matches(spark, conn, "events", root, ["id", "grp", "value"], "id")


def test_spark_inlined_deletes_without_flush(spark, ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE small AS SELECT i AS id FROM range(100) t(i)")
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    conn.execute("DELETE FROM small WHERE id IN (3, 50, 99)")
    _export(conn, flush=False)
    root = find_delta_roots(data_path)["small"]
    _assert_spark_matches(spark, conn, "small", root, ["id"], "id")


def test_spark_partitioned_table(spark, ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE sales (id INTEGER, region VARCHAR, amount DOUBLE)")
    conn.execute("ALTER TABLE sales SET PARTITIONED BY (region)")
    for batch in range(3):
        conn.execute(
            f"INSERT INTO sales SELECT i, ['east', 'west', 'north'][i % 3 + 1], i * 2.0 "
            f"FROM range({batch * 2000}, {(batch + 1) * 2000}) t(i)"
        )
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    conn.execute("UPDATE sales SET amount = amount + 1 WHERE id % 13 = 0")
    conn.execute("DELETE FROM sales WHERE id % 4 = 0")
    conn.execute("ALTER TABLE sales ADD COLUMN note VARCHAR")
    conn.execute(
        "MERGE INTO sales USING (SELECT i AS id FROM range(5900, 6100) t(i)) src ON sales.id = src.id "
        "WHEN MATCHED THEN UPDATE SET note = 'merged' "
        "WHEN NOT MATCHED THEN INSERT VALUES (src.id, 'east', 0, 'new')"
    )
    _export(conn)
    root = find_delta_roots(data_path)["sales"]
    _assert_spark_matches(spark, conn, "sales", root, ["id", "region", "amount", "note"], "id")


def test_spark_schema_evolution(spark, ducklake_env):
    """The issue #5 test_customers shape: renamed, dropped and widened columns plus partition evolution."""
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE customers (id INTEGER, name VARCHAR, city VARCHAR, score INTEGER)")
    conn.execute("INSERT INTO customers SELECT i, 'c' || i, ['a', 'b'][i % 2 + 1], i FROM range(500) t(i)")
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    conn.execute("ALTER TABLE customers RENAME COLUMN name TO full_name")
    conn.execute("ALTER TABLE customers DROP COLUMN city")
    conn.execute("ALTER TABLE customers ALTER COLUMN score TYPE BIGINT")
    conn.execute("ALTER TABLE customers ADD COLUMN tier VARCHAR")
    conn.execute("ALTER TABLE customers SET PARTITIONED BY (tier)")
    conn.execute("INSERT INTO customers SELECT i, 'c' || i, i * 1000000000, 'gold' FROM range(500, 600) t(i)")
    _export(conn)
    root = find_delta_roots(data_path)["customers"]
    _assert_spark_matches(spark, conn, "customers", root, ["id", "full_name", "score", "tier"], "id")


def test_spark_nested_types(spark, ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE nested (id INTEGER, tags VARCHAR[], info STRUCT(a INTEGER, b VARCHAR))")
    conn.execute(
        "INSERT INTO nested VALUES (1, ['x', 'y'], {'a': 10, 'b': 'ten'}), (2, [], {'a': 20, 'b': NULL})"
    )
    _export(conn)
    root = find_delta_roots(data_path)["nested"]
    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    rows = spark.read.format("delta").load(root).orderBy("id").collect()
    assert [(r.id, list(r.tags), r.info.a, r.info.b) for r in rows] == [(1, ["x", "y"], 10, "ten"), (2, [], 20, None)]


def test_spark_replaced_table_and_fresh_catalog(spark, extension_path, tmp_path):
    data_path = str(tmp_path / "data")
    meta = str(tmp_path / "meta.ducklake")

    def connect():
        conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        conn.execute(
            f"INSTALL '{extension_path}'; LOAD delta_export; INSTALL ducklake;"
            f"ATTACH 'ducklake:{meta}' AS test_lake (DATA_PATH '{data_path}'); USE test_lake;"
        )
        return conn

    conn = connect()
    for run, rows in enumerate([3000, 1700]):
        conn.execute(f"CREATE OR REPLACE TABLE sales AS SELECT i AS id, {run} AS run FROM range({rows}) t(i)")
        conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
        conn.execute("DELETE FROM sales WHERE id % 3 = 0")
        conn.execute("CALL ducklake_expire_snapshots('test_lake', older_than => now())")
        conn.execute("CALL ducklake_cleanup_old_files('test_lake', cleanup_all => true)")
        _export(conn)
        root = find_delta_roots(data_path)["sales"]
        _assert_spark_matches(spark, conn, "sales", root, ["id", "run"], "id")
    conn.close()

    for leftover in (meta, meta + ".wal"):
        if os.path.exists(leftover):
            os.remove(leftover)
    conn = connect()
    conn.execute("CREATE TABLE sales AS SELECT i AS id, 9 AS run FROM range(42) t(i)")
    _export(conn)
    _assert_spark_matches(spark, conn, "sales", root, ["id", "run"], "id")
    conn.close()


def test_spark_dropped_table_not_loadable(spark, ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE gone AS SELECT 1 AS x")
    _export(conn)
    root = find_delta_roots(data_path)["gone"]
    assert len(_spark_rows(spark, root, ["x"], "x")) == 1

    conn.execute("DROP TABLE gone")
    _export(conn)
    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    with pytest.raises(Exception, match="(?i)not a delta table|DELTA_MISSING_DELTA_TABLE|doesn't exist|is not a Delta"):
        spark.read.format("delta").load(root).collect()
