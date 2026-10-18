"""Checks that an exported Delta table reads back in Spark exactly like its DuckLake source.

Spark + Delta Lake (the Fabric Runtime 2.0 versions) is the oracle. assert_export_matches compares schema,
every row, the count Delta answers from file statistics, filtered counts (which exercise data skipping on the
exported min/max stats and the deletion vectors) and the protocol.
"""

import datetime
import json
import math
import os
import random
from collections import Counter
from decimal import Decimal

SIMPLE_TYPES = {
    "TINYINT": "tinyint",
    "SMALLINT": "smallint",
    "INTEGER": "int",
    "BIGINT": "bigint",
    "UTINYINT": "smallint",
    "USMALLINT": "int",
    "UINTEGER": "bigint",
    "FLOAT": "float",
    "DOUBLE": "double",
    "BOOLEAN": "boolean",
    "VARCHAR": "string",
    "BLOB": "binary",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamp",
}
FILTERABLE = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "DOUBLE",
              "BOOLEAN", "VARCHAR", "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE"}


def _split_top(s):
    parts, depth, cur = [], 0, ""
    for ch in s:
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur.strip())
    return parts


def spark_type(duck_type):
    """Spark simpleString of the Delta type delta_export writes for a DuckDB type."""
    t = duck_type.strip()
    if t.endswith("[]"):
        return f"array<{spark_type(t[:-2])}>"
    up = t.upper()
    if up.startswith("STRUCT("):
        fields = []
        for field in _split_top(t[7:-1]):
            if field.startswith('"'):
                end = field.index('"', 1)
                name, rest = field[1:end], field[end + 1:]
            else:
                name, rest = field.split(" ", 1)
            fields.append(f"{name}:{spark_type(rest)}")
        return f"struct<{','.join(fields)}>"
    if up.startswith("MAP("):
        key, value = _split_top(t[4:-1])
        return f"map<{spark_type(key)},{spark_type(value)}>"
    if up.startswith("DECIMAL"):
        p, s = (int(x) for x in up[up.index("(") + 1:-1].split(","))
        return f"decimal({p},{s})"
    return SIMPLE_TYPES[up]


def canonical(v):
    """Hashable, reader-independent form of a value (Spark Rows must already be converted to dicts)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, float):
        if math.isnan(v):
            return "NaN"
        return 0.0 if v == 0 else v
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v)
    if isinstance(v, datetime.datetime):
        return v.astimezone(datetime.timezone.utc).replace(tzinfo=None) if v.tzinfo else v
    if isinstance(v, dict):
        return ("dict", tuple(sorted(((canonical(k), canonical(x)) for k, x in v.items()), key=repr)))
    if isinstance(v, (list, tuple)):
        return tuple(canonical(x) for x in v)
    return v


def _spark_value(v):
    if hasattr(v, "asDict"):
        return {k: _spark_value(x) for k, x in v.asDict(recursive=False).items()}
    if isinstance(v, dict):
        return {_spark_value(k): _spark_value(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_spark_value(x) for x in v]
    return v


def duck_columns(conn, table):
    """[(name, duckdb type, nullable)] of a DuckLake table."""
    return [(r[0], r[1], r[2] == "YES") for r in conn.execute(f"DESCRIBE {table}").fetchall()]


def load(spark, root):
    # DeltaLog instances are cached per path; a re-export of the same folder must be re-read
    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    return spark.read.format("delta").load(root)


def delta_versions(root):
    log = os.path.join(root, "_delta_log")
    if not os.path.isdir(log):
        return []
    return sorted({int(f[:20]) for f in os.listdir(log) if f[:20].isdigit()})


def _quote(name):
    return '"' + name.replace('"', '""') + '"'


def _spark_col(name):
    from pyspark.sql import functions as F

    return F.col("`" + name.replace("`", "``") + "`")


def _spark_lit(v):
    """A Spark literal for a value read from DuckDB. Dates and timestamps go through ISO text: py4j formats
    year 1 as '1-01-01', which java.sql.Date rejects."""
    from pyspark.sql import functions as F

    if isinstance(v, datetime.datetime):
        if v.tzinfo:
            v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return F.lit(v.isoformat(sep=" ")).cast("timestamp")
    if isinstance(v, datetime.date):
        return F.lit(v.isoformat()).cast("date")
    return F.lit(v)


def _sort_key(v):
    """DuckDB and Spark order NaN above every number."""
    if isinstance(v, float) and math.isnan(v):
        return (1, 0.0)
    return (0, v)


def _matches(value, op, literal):
    """The predicate evaluated in Python over values both engines returned identically."""
    if op == "is_null":
        return value is None
    if value is None:
        return False
    value = canonical(value)
    if op == "in":
        return any(_matches(value, "=", x) for x in literal)
    literal = canonical(literal)
    a, b = _sort_key(value), _sort_key(literal)
    return {"=": a == b, "<": a < b, ">": a > b}[op]


# filter mismatches where Spark agrees with the truth and DuckLake does not (a DuckLake bug, not the export's)
DUCKLAKE_FILTER_DISAGREEMENTS = []


def _diff(expected, actual, limit=5):
    missing = list((expected - actual).elements())[:limit]
    extra = list((actual - expected).elements())[:limit]
    return f"missing from Delta: {missing}\nextra in Delta: {extra}"


def _checkpoint_adds(root):
    import pyarrow.parquet as pq

    with open(os.path.join(root, "_delta_log", "_last_checkpoint")) as f:
        version = json.load(f)["version"]
    table = pq.read_table(os.path.join(root, "_delta_log", f"{version:020d}.checkpoint.parquet"))
    return [a for a in table.column("add").to_pylist() if a is not None], table


def assert_export_matches(spark, conn, table, root, rng=None, label=""):
    """Every check from the plan; raises AssertionError with enough context to replay."""
    from pyspark.sql import functions as F

    rng = rng or random.Random(0)
    where = f"[{label}] {table} @ {root}"
    columns = duck_columns(conn, table)
    names = [c for c, _, _ in columns]
    df = load(spark, root)

    # schema: names, Delta types, nullability
    spark_fields = {f.name: f for f in df.schema.fields}
    assert list(spark_fields) == names, f"{where}: columns {list(spark_fields)} != {names}"
    for name, duck_type, nullable in columns:
        field = spark_fields[name]
        assert field.dataType.simpleString() == spark_type(duck_type), (
            f"{where}: {name} is {field.dataType.simpleString()}, expected {spark_type(duck_type)} for {duck_type}")
        assert field.nullable == nullable, f"{where}: {name} nullable {field.nullable} != {nullable}"

    # every row, as a multiset
    quoted = ", ".join(_quote(n) for n in names)
    expected = Counter(tuple(canonical(v) for v in row) for row in conn.execute(f"SELECT {quoted} FROM {table}").fetchall())
    collected = df.collect()
    actual = Counter(tuple(canonical(_spark_value(v)) for v in row) for row in collected)
    assert actual == expected, f"{where}: rows differ\n{_diff(expected, actual)}"

    # count answered from numRecords minus deletion vector cardinality
    assert df.count() == len(collected) == sum(expected.values()), f"{where}: count() {df.count()} != {len(collected)}"

    # filters: wrong min/max statistics or deletion vectors make data skipping drop rows
    filterable = [(n, t) for n, t, _ in columns if t.upper() in FILTERABLE or t.upper().startswith("DECIMAL")]
    for name, duck_type in rng.sample(filterable, min(4, len(filterable))):
        q = _quote(name)
        stats = conn.execute(
            f"SELECT min({q}), max({q}) FROM {table} WHERE {q} IS NOT NULL"
            + (f" AND isfinite({q})" if duck_type.upper() == "DOUBLE" else "")).fetchone()
        sample = [r[0] for r in conn.execute(
            f"SELECT {q} FROM {table} WHERE {q} IS NOT NULL"
            + (f" AND isfinite({q})" if duck_type.upper() == "DOUBLE" else "")
            + " USING SAMPLE 3 ROWS").fetchall()]
        literals = [v for v in [stats[0], stats[1], *sample] if v is not None]
        predicates = [("is_null", None)]
        for v in literals[:3]:
            predicates += [("=", v), ("<", v), (">", v)]
        if len(literals) >= 2:
            predicates.append(("in", literals[:2]))
        for op, v in rng.sample(predicates, min(5, len(predicates))):
            col = _spark_col(name)
            if op == "is_null":
                duck_where, spark_filter, params = f"{q} IS NULL", col.isNull(), []
            elif op == "in":
                duck_where, spark_filter, params = f"{q} IN (?, ?)", col.isin(*[_spark_lit(x) for x in v]), list(v)
            else:
                duck_where = f"{q} {op} ?"
                lit = _spark_lit(v)
                spark_filter = {"=": col == lit, "<": col < lit, ">": col > lit}[op]
                params = [v]
            want = conn.execute(f"SELECT count(*) FROM {table} WHERE {duck_where}", params).fetchone()[0]
            got = df.filter(spark_filter).count()
            if got == want:
                continue
            # arbitrate with the truth: the full row set is identical in both engines (checked above)
            index = names.index(name)
            truth = sum(1 for row in collected if _matches(_spark_value(row[index]), op, v))
            spark_ids = sorted(r[0] for r in df.filter(spark_filter).select(_spark_col(names[0])).collect())
            duck_ids = sorted(r[0] for r in conn.execute(
                f"SELECT {_quote(names[0])} FROM {table} WHERE {duck_where}", params).fetchall())
            detail = (f"{where}: filter {name} {op} {v!r}: Spark {got}, DuckLake {want}, truth {truth}; "
                      f"only Spark: {sorted(set(spark_ids) - set(duck_ids))[:10]}, "
                      f"only DuckLake: {sorted(set(duck_ids) - set(spark_ids))[:10]}")
            assert got == truth, detail
            print(f"\nDUCKLAKE FILTER DISAGREES (Spark matches the data): {detail}")
            DUCKLAKE_FILTER_DISAGREEMENTS.append(detail)

    # protocol: minimal, consistent with what the table uses
    adds, checkpoint = _checkpoint_adds(root)
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{root}`").collect()[0]
    has_dv = any(a.get("deletionVector") for a in adds)
    schema_string = next(m for m in checkpoint.column("metaData").to_pylist() if m)["schemaString"]
    has_widening = "delta.typeChanges" in schema_string
    features = set()
    if has_dv:
        features.add("deletionVectors")
    if has_widening:
        features.add("typeWidening")
    if features:
        assert (detail.minReaderVersion, detail.minWriterVersion) == (3, 7), f"{where}: protocol {detail}"
        assert set(detail.tableFeatures) == features, f"{where}: features {detail.tableFeatures} != {features}"
    else:
        assert (detail.minReaderVersion, detail.minWriterVersion) == (1, 2), f"{where}: protocol {detail}"
    return {"rows": len(collected), "files": len(adds), "dv": has_dv, "widening": has_widening}


def assert_not_loadable(spark, root, label=""):
    try:
        load(spark, root).collect()
    except Exception as e:  # AnalysisException: not a Delta table
        return str(e).splitlines()[0]
    raise AssertionError(f"[{label}] {root} still loads as a Delta table")
