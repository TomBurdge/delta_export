"""DuckLake metadata catalog discovery (GitHub issue #2).

DuckLake on DuckDB >= 1.5.3 attaches its metadata catalog HIDDEN unless METADATA_CATALOG is given,
so it no longer appears in duckdb_databases(). delta_export must still find it.
"""

import os

import duckdb


def _find_delta_log(data_path):
    for root, dirs, files in os.walk(data_path):
        if "_delta_log" in dirs:
            return os.path.join(root, "_delta_log")
    return None


def _connect(extension_path):
    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    conn.execute(f"INSTALL '{extension_path}'; LOAD delta_export; INSTALL ducklake; INSTALL delta;")
    return conn


def _delta_count(conn, data_path):
    delta_log = _find_delta_log(data_path)
    assert delta_log is not None, f"_delta_log not found under {data_path}"
    root = os.path.dirname(delta_log)
    return conn.execute(f"SELECT count(*) FROM delta_scan('{root}')").fetchone()[0]


def _run_hidden_default_attach(extension_path, tmp_path, attach_prefix, meta_file):
    """The reporter's script from issue #2, on either metadata backend."""
    meta = str(tmp_path / meta_file)
    data_path = str(tmp_path / "lake")
    conn = _connect(extension_path)
    conn.execute(
        f"ATTACH '{attach_prefix}{meta}' AS mylake (DATA_PATH '{data_path}');"
        f"CREATE SCHEMA mylake.mart;"
        f"CREATE TABLE mylake.mart.t AS SELECT i AS x FROM range(1, 51) t(i);"
        f"USE mylake;"
        f"CALL ducklake_flush_inlined_data('mylake');"
    )
    visible = conn.execute(
        "SELECT count(*) FROM duckdb_databases() WHERE starts_with(database_name, '__ducklake_metadata_')"
    ).fetchone()[0]
    assert visible == 0, "expected DuckLake to hide the metadata catalog on this DuckDB version"

    rows = conn.execute("CALL delta_export()").fetchall()
    assert [(r[0], r[1]) for r in rows] == [("mart.t", "needs_export")]
    assert _delta_count(conn, data_path) == 50
    conn.close()


def test_hidden_metadata_catalog_duckdb_backend(extension_path, tmp_path):
    _run_hidden_default_attach(extension_path, tmp_path, "ducklake:", "meta.ducklake")


def test_hidden_metadata_catalog_sqlite_backend(extension_path, tmp_path):
    _run_hidden_default_attach(extension_path, tmp_path, "ducklake:sqlite:", "meta.db")


def test_metadata_catalog_custom_name(extension_path, tmp_path):
    """METADATA_CATALOG 'lake_meta' does not carry the __ducklake_metadata_ prefix."""
    meta = str(tmp_path / "meta.ducklake")
    data_path = str(tmp_path / "lake")
    conn = _connect(extension_path)
    conn.execute(
        f"ATTACH 'ducklake:{meta}' AS mylake (DATA_PATH '{data_path}', METADATA_CATALOG 'lake_meta');"
        f"USE mylake;"
        f"CREATE TABLE t AS SELECT i AS x FROM range(1, 21) t(i);"
        f"CALL ducklake_flush_inlined_data('mylake');"
    )
    names = [r[0] for r in conn.execute("SELECT database_name FROM duckdb_databases()").fetchall()]
    assert "lake_meta" in names

    rows = conn.execute("CALL delta_export()").fetchall()
    assert [(r[0], r[1]) for r in rows] == [("main.t", "needs_export")]
    assert _delta_count(conn, data_path) == 20
    conn.close()


def test_metadata_schema_option(extension_path, tmp_path):
    """METADATA_SCHEMA puts the ducklake_* tables in a non-default schema of the metadata catalog."""
    meta = str(tmp_path / "meta.ducklake")
    data_path = str(tmp_path / "lake")
    conn = _connect(extension_path)
    conn.execute(
        f"ATTACH 'ducklake:{meta}' AS mylake (DATA_PATH '{data_path}', METADATA_SCHEMA 'meta');"
        f"USE mylake;"
        f"CREATE TABLE t AS SELECT i AS x FROM range(1, 21) t(i);"
        f"CALL ducklake_flush_inlined_data('mylake');"
    )
    rows = conn.execute("CALL delta_export()").fetchall()
    assert [(r[0], r[1]) for r in rows] == [("main.t", "needs_export")]
    assert _delta_count(conn, data_path) == 20
    conn.close()


def test_two_lakes_prefers_used_lake(extension_path, tmp_path):
    """With several lakes attached, the lake selected with USE is the one exported."""
    data_a = str(tmp_path / "lake_a")
    data_b = str(tmp_path / "lake_b")
    conn = _connect(extension_path)
    conn.execute(
        f"ATTACH 'ducklake:{tmp_path / 'a.ducklake'}' AS lake_a (DATA_PATH '{data_a}');"
        f"ATTACH 'ducklake:{tmp_path / 'b.ducklake'}' AS lake_b (DATA_PATH '{data_b}');"
        f"CREATE TABLE lake_a.main.ta AS SELECT i AS x FROM range(1, 11) t(i);"
        f"CREATE TABLE lake_b.main.tb AS SELECT i AS x FROM range(1, 31) t(i);"
        f"CALL ducklake_flush_inlined_data('lake_a');"
        f"CALL ducklake_flush_inlined_data('lake_b');"
        f"USE lake_b;"
    )
    rows = conn.execute("CALL delta_export()").fetchall()
    assert [(r[0], r[1]) for r in rows] == [("main.tb", "needs_export")]
    assert _find_delta_log(data_a) is None
    assert _delta_count(conn, data_b) == 30
    conn.close()
