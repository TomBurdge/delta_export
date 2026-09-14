"""DuckLake spec compliance (GitHub issue #1): path_is_relative, nulls_allowed, type mapping, nested types."""

import json
import os
import shutil


def _find_delta_log(data_path):
    for root, dirs, files in os.walk(data_path):
        if "_delta_log" in dirs:
            return os.path.join(root, "_delta_log")
    return None


def _checkpoint_path(delta_log):
    files = [f for f in os.listdir(delta_log) if f.endswith(".checkpoint.parquet")]
    assert len(files) == 1, f"expected one checkpoint in {delta_log}, got {files}"
    return os.path.join(delta_log, files[0])


def _schema(conn, delta_log):
    rows = conn.execute(
        f"SELECT metaData.schemaString FROM read_parquet('{_checkpoint_path(delta_log)}') WHERE metaData IS NOT NULL"
    ).fetchall()
    assert len(rows) == 1
    return json.loads(rows[0][0])


def _add_entries(conn, delta_log):
    return conn.execute(
        f"SELECT add.path, add.stats FROM read_parquet('{_checkpoint_path(delta_log)}') WHERE add IS NOT NULL"
    ).fetchall()


def _export(conn, data_path):
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    rows = conn.execute("CALL delta_export()").fetchall()
    delta_log = _find_delta_log(data_path)
    assert delta_log is not None, f"_delta_log not found under {data_path}"
    return rows, delta_log


def test_not_null_column_nullable_false(ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE t (id INTEGER NOT NULL, name VARCHAR)")
    conn.execute("INSERT INTO t VALUES (1, 'a'), (2, NULL)")
    _, delta_log = _export(conn, data_path)

    nullable = {f["name"]: f["nullable"] for f in _schema(conn, delta_log)["fields"]}
    assert nullable == {"id": False, "name": True}

    root = os.path.dirname(delta_log)
    assert conn.execute(f"SELECT count(*) FROM delta_scan('{root}')").fetchone()[0] == 2


def test_nested_types_schema_and_roundtrip(ducklake_env):
    conn, data_path = ducklake_env
    conn.execute(
        "CREATE TABLE nested ("
        "  id INTEGER,"
        "  tags VARCHAR[],"
        "  info STRUCT(a INTEGER, b VARCHAR),"
        "  attrs MAP(VARCHAR, INTEGER)"
        ")"
    )
    conn.execute(
        "INSERT INTO nested VALUES "
        "(1, ['x', 'y'], {'a': 10, 'b': 'ten'}, MAP {'k': 1}), "
        "(2, [], {'a': 20, 'b': NULL}, MAP {})"
    )
    _, delta_log = _export(conn, data_path)

    fields = _schema(conn, delta_log)["fields"]
    # child columns (element, a, b, key, value) must not leak into the top level
    assert [f["name"] for f in fields] == ["id", "tags", "info", "attrs"]
    types = {f["name"]: f["type"] for f in fields}
    assert types["id"] == "integer"
    assert types["tags"]["type"] == "array"
    assert types["tags"]["elementType"] == "string"
    assert isinstance(types["tags"]["containsNull"], bool)
    assert types["info"]["type"] == "struct"
    assert [(f["name"], f["type"]) for f in types["info"]["fields"]] == [("a", "integer"), ("b", "string")]
    assert all(set(f) == {"name", "type", "nullable", "metadata"} for f in types["info"]["fields"])
    assert types["attrs"]["type"] == "map"
    assert types["attrs"]["keyType"] == "string"
    assert types["attrs"]["valueType"] == "integer"
    assert isinstance(types["attrs"]["valueContainsNull"], bool)

    # stats must only reference top-level columns
    for _, stats in _add_entries(conn, delta_log):
        parsed = json.loads(stats)
        for key in ("minValues", "maxValues", "nullCount"):
            assert set(parsed.get(key, {})) <= {"id"}, f"{key} references nested children: {parsed[key]}"

    root = os.path.dirname(delta_log)
    rows = conn.execute(f"SELECT id, tags, info.a, info.b FROM delta_scan('{root}') ORDER BY id").fetchall()
    assert rows == [(1, ["x", "y"], 10, "ten"), (2, [], 20, None)]


def test_extra_type_mappings(ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE clean_types (d DECIMAL(18, 3), tz TIMESTAMPTZ)")
    conn.execute("INSERT INTO clean_types VALUES (12.345, '2024-01-01 12:00:00+00')")
    _, delta_log = _export(conn, data_path)

    types = {f["name"]: f["type"] for f in _schema(conn, delta_log)["fields"]}
    assert types == {"d": "decimal(18,3)", "tz": "timestamp"}
    root = os.path.dirname(delta_log)
    assert conn.execute(f"SELECT count(*) FROM delta_scan('{root}')").fetchone()[0] == 1


def test_lossy_type_mappings_are_explicit(ducklake_env):
    """uuid/time/json/interval have no Delta equivalent and are exported as 'string' (schema-level only)."""
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE lossy_types (u UUID, t TIME, j JSON, i INTERVAL)")
    conn.execute("INSERT INTO lossy_types VALUES (uuid(), '12:34:56', '{\"k\": 1}', INTERVAL 1 DAY)")
    _, delta_log = _export(conn, data_path)

    types = {f["name"]: f["type"] for f in _schema(conn, delta_log)["fields"]}
    assert types == {"u": "string", "t": "string", "j": "string", "i": "string"}


def test_absolute_data_file_path(ducklake_env, tmp_path):
    """ducklake_add_data_files keeps files outside the data path absolute (path_is_relative = false)."""
    conn, data_path = ducklake_env
    outside = tmp_path / "outside"
    outside.mkdir()
    ext_file = str(outside / "ext.parquet")
    conn.execute(f"COPY (SELECT i AS id, 'v' || i AS name FROM range(1, 11) t(i)) TO '{ext_file}' (FORMAT PARQUET)")
    conn.execute("CREATE TABLE ext (id BIGINT, name VARCHAR)")
    conn.execute(f"CALL ducklake_add_data_files('test_lake', 'ext', '{ext_file}')")

    # the hidden metadata catalog is still addressable by name
    files = conn.execute(
        'SELECT path, path_is_relative FROM "__ducklake_metadata_test_lake".ducklake_data_file '
        "WHERE end_snapshot IS NULL"
    ).fetchall()
    assert len(files) == 1
    stored_path, is_relative = files[0]
    assert is_relative is False, files

    rows, delta_log = _export(conn, data_path)
    assert [(r[0], r[1]) for r in rows] == [("main.ext", "needs_export")]
    add_paths = [p for p, _ in _add_entries(conn, delta_log)]
    assert add_paths == [stored_path], "absolute data file path must be emitted verbatim"
    assert os.path.isabs(add_paths[0])


def test_absolute_table_path(ducklake_env, tmp_path):
    """ducklake_table.path_is_relative = false: the path is the table root, not joined to data_path."""
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE moved (id BIGINT)")
    conn.execute("INSERT INTO moved SELECT i FROM range(1, 26) t(i)")
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")

    # locate the table directory through its data file
    file_name = conn.execute(
        'SELECT path FROM "__ducklake_metadata_test_lake".ducklake_data_file WHERE end_snapshot IS NULL'
    ).fetchone()[0]
    table_dir = None
    for root, dirs, files in os.walk(data_path):
        if os.path.basename(file_name) in files:
            table_dir = root
    assert table_dir is not None, f"{file_name} not found under {data_path}"

    new_dir = str(tmp_path / "elsewhere" / "moved")
    os.makedirs(os.path.dirname(new_dir))
    shutil.move(table_dir, new_dir)
    conn.execute(
        f'UPDATE "__ducklake_metadata_test_lake".ducklake_table '
        f"SET path = '{new_dir}/', path_is_relative = false "
        f"WHERE table_name = 'moved' AND end_snapshot IS NULL"
    )

    rows = conn.execute("CALL delta_export()").fetchall()
    assert [(r[0], r[1]) for r in rows] == [("main.moved", "needs_export")]
    assert os.path.isdir(os.path.join(new_dir, "_delta_log"))
    assert _find_delta_log(data_path) is None
    assert conn.execute(f"SELECT count(*) FROM delta_scan('{new_dir}')").fetchone()[0] == 25
