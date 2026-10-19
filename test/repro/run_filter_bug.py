"""Runs ducklake_filter_bug.sql and shows the filtered queries that miss rows, with literals and parameters."""

import os
import shutil
from decimal import Decimal

import duckdb

shutil.rmtree("/tmp/ducklake_filter_bug", ignore_errors=True)
os.makedirs("/tmp/ducklake_filter_bug")
conn = duckdb.connect()
script = open(os.path.join(os.path.dirname(__file__), "ducklake_filter_bug.sql")).read()
for statement in [s.strip() for s in script.split(";\n") if s.strip() and not s.strip().startswith("--")]:
    conn.execute(statement)

print(
    "duckdb",
    duckdb.__version__,
    "ducklake",
    conn.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake'").fetchone()[0],
)
value = Decimal("-49791089.6266012800")
for label, sql, params in [
    ("full scan, count_if", "SELECT count_if(c7 > -49791089.6266012800::DECIMAL(38,10)) FROM t2", []),
    ("filter, DECIMAL(38,10) literal", "SELECT count(*) FROM t2 WHERE c7 > -49791089.6266012800::DECIMAL(38,10)", []),
    ("filter, untyped literal", "SELECT count(*) FROM t2 WHERE c7 > -49791089.6266012800", []),
    ("filter, bound parameter", "SELECT count(*) FROM t2 WHERE c7 > ?", [value]),
    ("filter, count(id)", "SELECT count(id) FROM t2 WHERE c7 > -49791089.6266012800::DECIMAL(38,10)", []),
    ("filter, count(c7)", "SELECT count(c7) FROM t2 WHERE c7 > -49791089.6266012800::DECIMAL(38,10)", []),
    (
        "filter, rows fetched",
        "SELECT count(*) FROM (SELECT id FROM t2 WHERE c7 > -49791089.6266012800::DECIMAL(38,10)) s",
        [],
    ),
    ("filter, c7 IS NOT NULL", "SELECT count(*) FROM t2 WHERE c7 IS NOT NULL", []),
    ("full scan, count(c7)", "SELECT count(c7) FROM t2", []),
    ("filter, id >= 0", "SELECT count(*) FROM t2 WHERE id >= 0", []),
    ("full scan, count(*)", "SELECT count(*) FROM t2", []),
]:
    print(f"{label:34} {conn.execute(sql, params).fetchone()[0]}")

rows_filtered = {
    r[0] for r in conn.execute("SELECT id FROM t2 WHERE c7 > -49791089.6266012800::DECIMAL(38,10)").fetchall()
}
all_rows = {r[0]: r for r in conn.execute("SELECT id, grp, c7, c3, c2 FROM t2").fetchall()}
truth = {i for i, r in all_rows.items() if r[2] is not None and r[2] > value}
print("python exact count (c7 > value):", len(truth), " filtered ids:", len(rows_filtered))
print("only filtered:", sorted(rows_filtered - truth)[:10], " only python:", sorted(truth - rows_filtered)[:10])
print("min(c7), max(c7):", conn.execute("SELECT min(c7), max(c7) FROM t2").fetchone())
print(
    "rows with c7 = value (full scan count_if):",
    conn.execute("SELECT count_if(c7 = -49791089.6266012800::DECIMAL(38,10)) FROM t2").fetchone()[0],
)
print(
    "rows with c7 = value (filtered):",
    conn.execute("SELECT count(*) FROM t2 WHERE c7 = -49791089.6266012800::DECIMAL(38,10)").fetchone()[0],
)
print(
    "count_if over a materialised copy:",
    conn.execute(
        "CREATE TEMP TABLE copy AS SELECT * FROM t2; SELECT count_if(c7 > -49791089.6266012800::DECIMAL(38,10)), "
        "count(*) FILTER (c7 > -49791089.6266012800::DECIMAL(38,10)) FROM copy"
    ).fetchone(),
)
print(
    "filtered over the copy:",
    conn.execute("SELECT count(*) FROM copy WHERE c7 > -49791089.6266012800::DECIMAL(38,10)").fetchone()[0],
)
for label, sql in [
    ("t2:   SELECT c7 (only column)", "SELECT count(*), count(c7), sum(c7) FROM (SELECT c7 FROM t2)"),
    ("t2:   SELECT id, c7", "SELECT count(*), count(c7), sum(c7) FROM (SELECT id, c7 FROM t2)"),
    ("t2:   SELECT *", "SELECT count(*), count(c7), sum(c7) FROM (SELECT * FROM t2)"),
    ("copy: SELECT c7", "SELECT count(*), count(c7), sum(c7) FROM (SELECT c7 FROM copy)"),
]:
    print(f"{label:30}", conn.execute(sql).fetchone())
extra = conn.execute(
    "SELECT c7, count(*) FROM (SELECT c7 FROM t2) GROUP BY c7 "
    "EXCEPT ALL SELECT c7, count(*) FROM (SELECT c7 FROM copy) GROUP BY c7"
).fetchall()
print("c7 values the single-column scan returns too often:", extra[:5])
meta = '"__ducklake_metadata_lake"'
print("actual min/max of c7:", conn.execute("SELECT min(c7), max(c7) FROM copy").fetchone())
print(
    "ducklake_table_column_stats for c7:",
    conn.execute(
        f"SELECT s.* FROM {meta}.ducklake_table_column_stats s JOIN {meta}.ducklake_column c "
        f"ON c.table_id = s.table_id AND c.column_id = s.column_id AND c.end_snapshot IS NULL "
        f"WHERE c.column_name = 'c7'"
    ).fetchall(),
)
print(
    "ducklake_file_column_stats for c7 (active files):",
    conn.execute(
        f"SELECT f.data_file_id, f.min_value, f.max_value, f.null_count FROM {meta}.ducklake_file_column_stats f "
        f"JOIN {meta}.ducklake_data_file d ON d.data_file_id = f.data_file_id AND d.end_snapshot IS NULL "
        f"JOIN {meta}.ducklake_column c ON c.table_id = d.table_id AND c.column_id = f.column_id "
        f"AND c.end_snapshot IS NULL WHERE c.column_name = 'c7' ORDER BY 1"
    ).fetchall(),
)
print(conn.execute("EXPLAIN SELECT count_if(c7 > -49791089.6266012800::DECIMAL(38,10)) FROM t2").fetchall()[0][1])
print("ids the filtered query misses:", sorted(truth - rows_filtered)[:20])
for i in sorted(truth - rows_filtered)[:5]:
    print("  ", all_rows[i])
print(
    "files:",
    conn.execute(
        "SELECT data_file, data_file_size_bytes, delete_file FROM ducklake_list_files('lake', 't2')"
    ).fetchall(),
)
print(conn.execute("EXPLAIN SELECT count(*) FROM t2 WHERE c7 > -49791089.6266012800::DECIMAL(38,10)").fetchall()[0][1])
