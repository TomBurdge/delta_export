"""Independent Delta readers used as oracles next to DuckDB's delta_scan (delta-kernel-rs).

delta-rs runs in the plain python-tests job; Spark, the reference reader, runs in test_spark_oracle.py.
"""

import os

import pyarrow as pa


def find_delta_roots(data_path):
    """{table folder name: table root} for every exported table under data_path."""
    roots = {}
    for root, dirs, files in os.walk(data_path):
        if "_delta_log" in dirs and os.path.exists(os.path.join(root, "_delta_log", "_last_checkpoint")):
            roots[os.path.basename(root)] = root
    return roots


def _delta_rs_query(root, sql):
    from deltalake import DeltaTable, QueryBuilder

    result = QueryBuilder().register("t", DeltaTable(root)).execute(sql)
    table = result.read_all() if hasattr(result, "read_all") else pa.Table.from_batches(result)
    # deltalake 1.x returns arro3 tables; pyarrow reads them through the Arrow C stream interface
    return table if isinstance(table, pa.Table) else pa.table(table)


def delta_rs_count(root):
    return _delta_rs_query(root, "SELECT count(*) AS n FROM t").column(0)[0].as_py()


def delta_rs_rows(root, columns, order_by):
    table = _delta_rs_query(root, f"SELECT {', '.join(columns)} FROM t ORDER BY {order_by}")
    return [tuple(row.values()) for row in table.to_pylist()]
