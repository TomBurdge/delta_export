"""Random DuckLake workloads for the delta_export fuzz tests.

A Fuzzer drives one DuckLake catalog through a reproducible random sequence of DDL, DML and maintenance
calls and keeps a small model of every table (columns, partitioning, whether a rename or a drop + re-add
happened). It calls on_export(fuzzer, rows) whenever the sequence reaches a delta_export() step; the
caller compares the export with an independent Delta reader.
"""

import random

import duckdb

# DuckDB types the fuzzer puts in tables. Types delta_export maps to a lossy 'string' (uint64, hugeint,
# uuid, time, interval) are left out on purpose: Delta has no type for them.
PRIMITIVE_TYPES = [
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER",
    "FLOAT", "DOUBLE", "DECIMAL(9,2)", "DECIMAL(18,6)", "DECIMAL(38,10)",
    "BOOLEAN", "VARCHAR", "BLOB", "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE",
]
NESTED_TYPES = ["INTEGER[]", "STRUCT(a INTEGER, b VARCHAR)", "MAP(VARCHAR, INTEGER)"]

# DuckLake type promotions that are also Delta type widenings (u8 -> short, u16 -> integer, u32 -> long)
WIDENINGS = {
    "TINYINT": ["SMALLINT", "INTEGER", "BIGINT"],
    "SMALLINT": ["INTEGER", "BIGINT"],
    "INTEGER": ["BIGINT"],
    "UTINYINT": ["USMALLINT", "UINTEGER"],
    "USMALLINT": ["UINTEGER"],
    "FLOAT": ["DOUBLE"],
}

PARTITIONABLE = {"TINYINT", "SMALLINT", "INTEGER", "BOOLEAN", "DATE"}
COLUMN_NAMES = [f"c{i}" for i in range(10)]


def value_expr(duck_type, row, salt, seed):
    """SQL expression for a pseudo-random value of duck_type, ~10% NULL, with extremes and specials."""
    h = f"hash({row}, {salt}, {seed})"
    v = f"hash({row}, {salt + 7919}, {seed})"  # independent of the NULL decision
    t = duck_type.upper()
    ints = {
        "TINYINT": (-128, 127, 256),
        "SMALLINT": (-32768, 32767, 65536),
        "INTEGER": (-2147483648, 2147483647, 4294967296),
        "BIGINT": (-9223372036854775808, 9223372036854775807, 2000000000000000000),
        "UTINYINT": (0, 255, 256),
        "USMALLINT": (0, 65535, 65536),
        "UINTEGER": (0, 4294967295, 4294967296),
    }
    if t in ints:
        lo, hi, span = ints[t]
        offset = 0 if t.startswith("U") else (span // 2)
        inner = (f"CASE {v} % 20 WHEN 0 THEN ({lo})::{t} WHEN 1 THEN ({hi})::{t} "
                 f"ELSE (({v} % {span})::HUGEINT - {offset})::{t} END")
    elif t in ("FLOAT", "DOUBLE"):
        inner = (f"CASE {v} % 25 WHEN 0 THEN 'nan'::{t} WHEN 1 THEN 'inf'::{t} WHEN 2 THEN '-inf'::{t} "
                 f"WHEN 3 THEN '-0.0'::{t} ELSE ((({v} % 2000000)::DOUBLE - 1000000) / 7)::{t} END")
    elif t.startswith("DECIMAL"):
        precision, scale = (int(x) for x in t[t.index("(") + 1:-1].split(","))
        modulus = 10 ** min(precision, 18)
        inner = (f"((({v} % {modulus})::HUGEINT - {modulus // 2})::DECIMAL(38,0) / {10 ** scale})"
                 f"::DECIMAL({precision},{scale})")
    elif t == "BOOLEAN":
        inner = f"{v} % 2 = 0"
    elif t == "VARCHAR":
        inner = (f"CASE {v} % 7 WHEN 0 THEN '' WHEN 1 THEN 'é漢字🙂' || ({v} % 100) "
                 f"WHEN 2 THEN 'it''s \"q\" ' || ({v} % 100) WHEN 3 THEN repeat('x', 1000) || ({v} % 100) "
                 f"WHEN 4 THEN repeat('p', 40) || ({v} % 1000) ELSE 'v' || ({v} % 5000) END")
    elif t == "BLOB":
        inner = f"('b' || ({v} % 1000))::BLOB"
    elif t == "DATE":
        inner = (f"CASE {v} % 20 WHEN 0 THEN DATE '0001-01-01' WHEN 1 THEN DATE '9999-12-31' "
                 f"WHEN 2 THEN DATE '1900-01-01' "
                 f"ELSE DATE '1970-01-01' + (({v} % 40000)::BIGINT - 20000)::INTEGER END")
    elif t in ("TIMESTAMP", "TIMESTAMP WITH TIME ZONE"):
        ts = (f"CASE {v} % 20 WHEN 0 THEN TIMESTAMP '1900-01-01 00:00:00.000001' "
              f"WHEN 1 THEN TIMESTAMP '9999-12-31 23:59:59.999999' "
              f"ELSE TIMESTAMP '1950-01-01' + to_microseconds(({v} % 3000000000000000)::BIGINT) END")
        inner = ts if t == "TIMESTAMP" else f"({ts})::TIMESTAMPTZ"
    elif t == "INTEGER[]":
        inner = (f"CASE WHEN {v} % 5 = 0 THEN []::INTEGER[] "
                 f"ELSE [({v} % 100)::INTEGER, ({v} % 7)::INTEGER] END")
    elif t.startswith("STRUCT"):
        inner = f"{{'a': ({v} % 100)::INTEGER, 'b': 'v' || ({v} % 5)}}::{duck_type}"
    elif t.startswith("MAP"):
        inner = f"MAP {{('k' || ({v} % 3)): ({v} % 100)::INTEGER}}"
    else:
        raise ValueError(f"no generator for {duck_type}")
    return f"CASE WHEN {h} % 10 = 0 THEN NULL ELSE ({inner}) END"


def quote(name):
    return '"' + name.replace('"', '""') + '"'


class TableModel:
    def __init__(self, schema, name, columns):
        self.schema = schema
        self.name = name
        self.columns = columns  # list of [name, type]; id BIGINT and grp INTEGER always first
        self.next_id = 0
        self.partition = None
        self.risky = False  # a rename or a drop + re-add under the same name since the table was (re)built
        self.dropped_names = set()
        self.salt = 0

    @property
    def qualified(self):
        return f"{quote(self.schema)}.{quote(self.name)}"

    def mutable_columns(self):
        return [c for c in self.columns if c[0] not in ("id", "grp")]


class Fuzzer:
    def __init__(self, conn, seed, catalog="test_lake", on_export=None, max_ops=None):
        self.conn = conn
        self.seed = seed
        self.catalog = catalog
        self.rng = random.Random(seed)
        self.on_export = on_export or (lambda fuzzer, rows: None)
        self.tables = {}  # (schema, name) -> TableModel
        self.dropped = {}  # (schema, name) -> TableModel, dropped and not recreated
        self.log = []
        self.max_ops = max_ops or self.rng.randint(25, 45)
        self.table_counter = 0

    # ── helpers ──

    def sql(self, statement):
        self.log.append(statement)
        return self.conn.execute(statement)

    def try_sql(self, statement):
        """Mutations DuckLake may legitimately refuse (e.g. altering a partition column)."""
        try:
            self.sql(statement)
            return True
        except duckdb.Error as e:
            self.log.append(f"  -- refused: {str(e).splitlines()[0][:200]}")
            return False

    def next_salt(self, table):
        table.salt += 1
        return table.salt * 131 + self.seed % 1000

    def random_type(self, nested_ok=True):
        pool = PRIMITIVE_TYPES + (NESTED_TYPES if nested_ok else [])
        return self.rng.choice(pool)

    def pick_table(self):
        return self.rng.choice(list(self.tables.values())) if self.tables else None

    # ── ops ──

    def op_create_table(self, schema=None, name=None):
        schema = schema or self.rng.choice(self.schemas)
        if name is None:
            self.table_counter += 1
            name = f"t{self.table_counter}"
        names = self.rng.sample(COLUMN_NAMES, self.rng.randint(2, 6))
        columns = [["id", "BIGINT"], ["grp", "INTEGER"]] + [[n, self.random_type()] for n in names]
        table = TableModel(schema, name, columns)
        ddl = ", ".join(f"{quote(c)} {t}" for c, t in columns)
        self.sql(f"CREATE TABLE {table.qualified} ({ddl})")
        self.tables[(schema, name)] = table
        self.dropped.pop((schema, name), None)
        self.op_insert(table, self.rng.choice([1, 50, 1000, 5000]))
        return table

    def op_insert(self, table=None, rows=None):
        table = table or self.pick_table()
        if table is None:
            return
        rows = rows or self.rng.choice([1, 10, 300, 2000, 8000, 20000])
        start = table.next_id
        exprs = []
        for c, t in table.columns:
            if c == "id":
                exprs.append(f"{start} + i")
            elif c == "grp":
                exprs.append(f"(({start} + i) % 7)::INTEGER")
            else:
                exprs.append(value_expr(t, f"{start} + i", self.next_salt(table), self.seed))
        cols = ", ".join(quote(c) for c, _ in table.columns)
        if self.try_sql(f"INSERT INTO {table.qualified} ({cols}) SELECT {', '.join(exprs)} FROM range({rows}) r(i)"):
            table.next_id += rows

    def random_predicate(self, table):
        kind = self.rng.random()
        if kind < 0.4:
            return f"hash(id, {self.rng.randint(0, 10 ** 6)}) % {self.rng.randint(2, 20)} = 0"
        if kind < 0.7 and table.next_id:
            a = self.rng.randrange(table.next_id)
            return f"id BETWEEN {a} AND {a + self.rng.randint(0, max(1, table.next_id // 5))}"
        if kind < 0.85:
            return f"grp = {self.rng.randrange(7)}"
        return "id % 2 = 1"

    def op_delete(self):
        table = self.pick_table()
        if table:
            self.try_sql(f"DELETE FROM {table.qualified} WHERE {self.random_predicate(table)}")

    def op_delete_all(self):
        table = self.pick_table()
        if table:
            self.try_sql(f"DELETE FROM {table.qualified}")

    def op_update(self):
        table = self.pick_table()
        if not table or not table.mutable_columns():
            return
        c, t = self.rng.choice(table.mutable_columns())
        expr = value_expr(t, "id", self.next_salt(table), self.seed)
        self.try_sql(f"UPDATE {table.qualified} SET {quote(c)} = {expr} WHERE {self.random_predicate(table)}")

    def op_merge(self):
        table = self.pick_table()
        if not table or not table.mutable_columns():
            return
        c, t = self.rng.choice(table.mutable_columns())
        lo = self.rng.randrange(max(1, table.next_id))
        hi = lo + self.rng.randint(1, 3000)
        expr = value_expr(t, "src.id", self.next_salt(table), self.seed)
        # DuckLake MERGE supports a single UPDATE or DELETE action
        matched = "DELETE" if self.rng.random() < 0.3 else f"UPDATE SET {quote(c)} = {expr}"
        statement = (
            f"MERGE INTO {table.qualified} AS tgt USING (SELECT i AS id FROM range({lo}, {hi}) r(i)) AS src "
            f"ON tgt.id = src.id "
            f"WHEN MATCHED THEN {matched} "
            f"WHEN NOT MATCHED THEN INSERT (id, grp) VALUES (src.id, (src.id % 7)::INTEGER)"
        )
        if self.try_sql(statement):
            table.next_id = max(table.next_id, hi)

    def op_add_column(self):
        table = self.pick_table()
        if not table:
            return
        free = [n for n in COLUMN_NAMES if n not in {c for c, _ in table.columns}]
        if not free:
            return
        name, t = self.rng.choice(free), self.random_type()
        if self.try_sql(f"ALTER TABLE {table.qualified} ADD COLUMN {quote(name)} {t}"):
            table.columns.append([name, t])
            if name in table.dropped_names:
                table.risky = True

    def op_drop_column(self):
        table = self.pick_table()
        candidates = [c for c in (table.mutable_columns() if table else []) if c[0] != table.partition]
        if not candidates:
            return
        c, _ = self.rng.choice(candidates)
        if self.try_sql(f"ALTER TABLE {table.qualified} DROP COLUMN {quote(c)}"):
            table.columns = [col for col in table.columns if col[0] != c]
            table.dropped_names.add(c)

    def op_rename_column(self):
        table = self.pick_table()
        candidates = [c for c in (table.mutable_columns() if table else []) if c[0] != table.partition]
        if not candidates:
            return
        col = self.rng.choice(candidates)
        new = f"{col[0]}_r{self.rng.randint(0, 99)}"
        if new in {c for c, _ in table.columns}:
            return
        if self.try_sql(f"ALTER TABLE {table.qualified} RENAME COLUMN {quote(col[0])} TO {quote(new)}"):
            table.dropped_names.add(col[0])
            col[0] = new
            table.risky = True

    def op_widen(self):
        table = self.pick_table()
        candidates = [c for c in (table.mutable_columns() if table else []) if c[1] in WIDENINGS]
        if not candidates:
            return
        col = self.rng.choice(candidates)
        target = self.rng.choice(WIDENINGS[col[1]])
        if self.try_sql(f"ALTER TABLE {table.qualified} ALTER COLUMN {quote(col[0])} TYPE {target}"):
            col[1] = target

    def op_partition(self):
        table = self.pick_table()
        if not table:
            return
        if table.partition and self.rng.random() < 0.3:
            if self.try_sql(f"ALTER TABLE {table.qualified} RESET PARTITIONED BY"):
                table.partition = None
            return
        candidates = [c for c, t in table.columns if t in PARTITIONABLE]
        c = self.rng.choice(candidates)
        if self.try_sql(f"ALTER TABLE {table.qualified} SET PARTITIONED BY ({quote(c)})"):
            table.partition = c

    def op_flush(self):
        self.try_sql(f"CALL ducklake_flush_inlined_data('{self.catalog}')")

    def op_rewrite(self):
        threshold = self.rng.choice([0, 0.25, 0.5, 0.95])
        self.try_sql(f"CALL {self.catalog}.set_option('rewrite_delete_threshold', {threshold})")
        self.try_sql(f"CALL ducklake_rewrite_data_files('{self.catalog}')")

    def op_merge_files(self):
        self.try_sql(f"CALL ducklake_merge_adjacent_files('{self.catalog}')")

    def op_expire(self):
        self.try_sql(f"CALL ducklake_expire_snapshots('{self.catalog}', older_than => now())")
        self.try_sql(f"CALL ducklake_cleanup_old_files('{self.catalog}', cleanup_all => true)")

    def op_replace(self):
        """CREATE OR REPLACE ... AS SELECT: rewrites every file with the current names (the rename fix)."""
        table = self.pick_table()
        if table and self.try_sql(f"CREATE OR REPLACE TABLE {table.qualified} AS SELECT * FROM {table.qualified}"):
            table.risky = False
            table.dropped_names = set()
            table.partition = None

    def op_drop_table(self):
        table = self.pick_table()
        if table and len(self.tables) > 1 and self.try_sql(f"DROP TABLE {table.qualified}"):
            del self.tables[(table.schema, table.name)]
            self.dropped[(table.schema, table.name)] = table

    def op_recreate_table(self):
        table = self.pick_table()
        if table and self.try_sql(f"DROP TABLE {table.qualified}"):
            del self.tables[(table.schema, table.name)]
            self.op_create_table(table.schema, table.name)

    def op_export(self):
        self.log.append("CALL delta_export()")
        rows = self.conn.execute("CALL delta_export()").fetchall()
        self.on_export(self, rows)

    OPS = [
        ("op_insert", 14), ("op_delete", 10), ("op_update", 8), ("op_merge", 6), ("op_delete_all", 1),
        ("op_add_column", 4), ("op_drop_column", 3), ("op_rename_column", 2), ("op_widen", 4),
        ("op_partition", 3), ("op_flush", 6), ("op_rewrite", 3), ("op_merge_files", 2), ("op_expire", 2),
        ("op_replace", 2), ("op_drop_table", 1), ("op_recreate_table", 1), ("op_create_table", 2),
        ("op_export", 6),
    ]

    def run(self):
        self.schemas = ["main"] + [f"s{i}" for i in range(self.rng.randint(0, 2))]
        for schema in self.schemas[1:]:
            self.sql(f"CREATE SCHEMA {quote(schema)}")
        for _ in range(self.rng.randint(1, 4)):
            self.op_create_table()
        names, weights = zip(*self.OPS)
        for _ in range(self.max_ops):
            getattr(self, self.rng.choices(names, weights)[0])()
        # always end with a flush and an export so the final state is checked
        self.op_flush()
        self.op_export()

    def replay(self):
        return "\n".join(self.log)
