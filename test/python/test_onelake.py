"""Live OneLake round trip, checked through the Fabric SQL analytics endpoint.

DuckLake data lives on abfss:// (the lakehouse in ONELAKE_WAREHOUSE=<workspace-guid>/<lakehouse-guid>),
delta_export() writes _delta_log there, and the lakehouse's SQL analytics endpoint must then read every
exported table with the same counts and aggregates as DuckLake: deletion vectors, partitions, a widened column,
a dropped table that disappears and a renamed table that is held back instead of showing NULLs.

Auth is GitHub Actions OIDC only (workload-identity federation, no client secret). Skipped unless running in
the onelake-tests CI job.
"""

import json
import os
import posixpath
import struct
import time
import urllib.error
import urllib.request

import duckdb
import pytest

OIDC_ENV = ("AZURE_CLIENT_ID", "AZURE_TENANT_ID", "ACTIONS_ID_TOKEN_REQUEST_URL")
pytestmark = pytest.mark.skipif(
    not os.environ.get("ONELAKE_WAREHOUSE"),
    reason="live OneLake test: runs only in the onelake-tests CI job",
)

SCHEMA = "delta_export_it"
FABRIC_API = "https://api.fabric.microsoft.com/v1"
SQL_COPT_SS_ACCESS_TOKEN = 1256
PERMISSION_HINT = (
    "the fabric-github-deploy service principal needs access to the workspace (Contributor or Member) and the "
    "tenant setting 'Service principals can use Fabric APIs' must allow it"
)


def _github_oidc_assertion():
    url = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"]
    sep = "&" if "?" in url else "?"
    request = urllib.request.Request(
        f"{url}{sep}audience=api://AzureADTokenExchange",
        headers={"Authorization": f"Bearer {os.environ['ACTIONS_ID_TOKEN_REQUEST_TOKEN']}"},
    )
    with urllib.request.urlopen(request, timeout=15) as resp:
        return json.loads(resp.read().decode())["value"]


_credential = None


def _token(scope):
    global _credential
    from azure.identity import ClientAssertionCredential

    missing = [v for v in OIDC_ENV if not os.environ.get(v)]
    # The job sets ONELAKE_WAREHOUSE, so missing secrets / id-token permission must fail, not skip
    assert not missing, f"OIDC not configured, missing: {missing}"
    if _credential is None:
        _credential = ClientAssertionCredential(
            os.environ["AZURE_TENANT_ID"], os.environ["AZURE_CLIENT_ID"], _github_oidc_assertion
        )
    return _credential.get_token(scope).token


def _fabric(method, path_or_url, body=None):
    url = path_or_url if path_or_url.startswith("https://") else FABRIC_API + path_or_url
    request = urllib.request.Request(
        url,
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {_token('https://api.fabric.microsoft.com/.default')}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as resp:
            text = resp.read().decode()
            return resp.status, dict(resp.headers), json.loads(text) if text else {}
    except urllib.error.HTTPError as e:
        detail = e.read().decode()[:500]
        if e.code in (401, 403):
            pytest.fail(f"Fabric API {method} {url} returned {e.code}: {detail}\n{PERMISSION_HINT}")
        raise AssertionError(f"Fabric API {method} {url} returned {e.code}: {detail}")


class SqlEndpoint:
    def __init__(self, workspace, lakehouse):
        self.workspace = workspace
        _, _, item = _fabric("GET", f"/workspaces/{workspace}/lakehouses/{lakehouse}")
        props = item.get("properties", {}).get("sqlEndpointProperties") or {}
        assert props.get("id") and props.get("connectionString"), f"lakehouse has no SQL endpoint yet: {item}"
        self.id = props["id"]
        self.server = props["connectionString"]
        self.database = item["displayName"]
        self._conn = None

    def sync(self, schema, tables):
        """Refresh SQL Endpoint Metadata for these tables only and wait for it: {schema.table: status, error}.

        POST .../refreshMetadata answers 200 with the per-table statuses, or 202 with a long running operation
        whose result (GET <Location>/result) holds them.
        """
        body = {"tables": [{"schema": schema, "tableNames": list(tables)}],
                "timeout": {"timeUnit": "Minutes", "value": 10}}
        status, headers, result = _fabric("POST", f"/workspaces/{self.workspace}/sqlEndpoints/{self.id}/refreshMetadata", body)
        if status == 202:
            location = headers.get("Location") or headers.get("location")
            deadline = time.time() + 900
            while True:
                time.sleep(int(headers.get("Retry-After", "5")))
                _, headers, operation = _fabric("GET", location)
                state = operation.get("status")
                if state == "Succeeded":
                    break
                if state == "Failed" or time.time() > deadline:
                    raise AssertionError(f"SQL endpoint metadata refresh {state}: {operation}")
            _, _, result = _fabric("GET", location.rstrip("/") + "/result")
        statuses = {}
        for entry in result.get("value", []):
            name = entry["tableName"].split(".", 1)[-1]
            statuses[name] = (entry["status"], (entry.get("error") or {}).get("message"))
        print(f"SQL endpoint sync: {statuses}")
        return statuses

    def query(self, sql, params=()):
        if self._conn is None:
            import mssql_python

            token = _token("https://database.windows.net/.default").encode("utf-16-le")
            self._conn = mssql_python.connect(
                f"Server={self.server},1433;Database={self.database};Encrypt=yes;TrustServerCertificate=no;",
                attrs_before={SQL_COPT_SS_ACCESS_TOKEN: struct.pack(f"<I{len(token)}s", len(token), token)},
            )
        cursor = self._conn.cursor()
        cursor.execute(sql, params) if params else cursor.execute(sql)
        return [tuple(r) for r in cursor.fetchall()]

    def tables(self, prefix):
        return {r[1]: r[0] for r in self.query(
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE ?", (prefix + "%",))}


@pytest.fixture
def onelake(extension_path, tmp_path):
    workspace, lakehouse = os.environ["ONELAKE_WAREHOUSE"].strip("/").split("/")
    data_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}/Tables/"
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(
        f"INSTALL '{extension_path}';"
        f"LOAD delta_export;"
        f"INSTALL ducklake;"
        f"INSTALL azure;"
        f"LOAD azure;"
        f"INSTALL delta;"
    )
    # The default transport fails the OneLake TLS handshake on Linux ("Problem with the SSL CA cert");
    # curl uses the system CA bundle
    con.execute("SET azure_transport_option_type = 'curl'")
    con.execute(
        f"CREATE SECRET onelake (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{_token('https://storage.azure.com/.default')}')"
    )
    con.execute(f"ATTACH 'ducklake:{tmp_path / 'meta.ducklake'}' AS test_lake (DATA_PATH '{data_path}')")
    con.execute("USE test_lake")
    yield con, workspace, lakehouse, data_path
    con.close()


def _export(conn):
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    rows = conn.execute("SELECT * FROM delta_export()").fetchall()
    print(f"delta_export: {rows}")
    return {r[0]: r[1] for r in rows}


def test_onelake_export_read_by_sql_endpoint(onelake):
    conn, workspace, lakehouse, data_path = onelake
    prefix = f"t_{os.environ.get('GITHUB_RUN_ID', 'local')}_{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}_"
    dv, part, widen, renamed, dropped = (prefix + n for n in ("dv", "part", "widen", "renamed", "dropped"))
    q = lambda t: f"{SCHEMA}.{t}"  # noqa: E731
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    try:
        # deletion vectors: deletes, update and merge without any rewrite
        conn.execute(f"CREATE TABLE {q(dv)} AS SELECT i AS id, (i % 7)::INTEGER AS grp, 'n' || (i % 500) AS name "
                     f"FROM range(20000) t(i)")
        conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
        conn.execute(f"DELETE FROM {q(dv)} WHERE id % 7 = 0")
        conn.execute(f"UPDATE {q(dv)} SET name = 'updated' WHERE id % 11 = 0")
        # partitioned, with deletes
        conn.execute(f"CREATE TABLE {q(part)} (id BIGINT, region VARCHAR, amount INTEGER)")
        conn.execute(f"ALTER TABLE {q(part)} SET PARTITIONED BY (region)")
        conn.execute(f"INSERT INTO {q(part)} SELECT i, ['east', 'west', 'north'][i % 3 + 1], i % 1000 FROM range(9000) t(i)")
        conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
        conn.execute(f"DELETE FROM {q(part)} WHERE id % 4 = 0")
        # widened column: old files hold INT32
        conn.execute(f"CREATE TABLE {q(widen)} (id BIGINT, score INTEGER)")
        conn.execute(f"INSERT INTO {q(widen)} SELECT i, i FROM range(1000) t(i)")
        conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
        conn.execute(f"ALTER TABLE {q(widen)} ALTER COLUMN score TYPE BIGINT")
        conn.execute(f"INSERT INTO {q(widen)} SELECT i, i * 10000000000 FROM range(1000, 1100) t(i)")
        # renamed later, and dropped later
        conn.execute(f"CREATE TABLE {q(renamed)} AS SELECT i AS id, 'n' || i AS name FROM range(100) t(i)")
        conn.execute(f"CREATE TABLE {q(dropped)} AS SELECT i AS id FROM range(10) t(i)")

        status = _export(conn)
        assert all(status[q(t)] == "needs_export" for t in (dv, part, widen, renamed, dropped)), status

        endpoint = SqlEndpoint(workspace, lakehouse)
        synced = endpoint.sync(SCHEMA, [dv, part, widen, renamed, dropped])
        assert all(synced.get(t, ("missing",))[0] == "Success" for t in (dv, part, widen, renamed, dropped)), synced
        seen = endpoint.tables(prefix)
        assert {dv, part, widen, renamed, dropped} <= set(seen), seen

        # second round: more deletes, a rename (held back) and a drop (log removed)
        conn.execute(f"MERGE INTO {q(dv)} AS tgt USING (SELECT i AS id FROM range(19900, 20100) t(i)) AS src "
                     f"ON tgt.id = src.id WHEN MATCHED THEN DELETE "
                     f"WHEN NOT MATCHED THEN INSERT VALUES (src.id, (src.id % 7)::INTEGER, 'merged')")
        conn.execute(f"ALTER TABLE {q(renamed)} RENAME COLUMN name TO full_name")
        conn.execute(f"INSERT INTO {q(renamed)} VALUES (1000, 'new')")
        conn.execute(f"DROP TABLE {q(dropped)}")
        status = _export(conn)
        assert status[q(renamed)] == "needs_rewrite", status
        assert status[q(dropped)] == "removed_dropped", status
        assert all(status[q(t)] == "needs_export" for t in (dv, part, widen)), status

        # delta_scan over abfss:// agrees before asking Fabric
        for t in (dv, part, widen):
            root = posixpath.join(data_path, SCHEMA, t)
            assert conn.execute(f"SELECT count(*) FROM delta_scan('{root}')").fetchone()[0] == \
                conn.execute(f"SELECT count(*) FROM {q(t)}").fetchone()[0], t

        # sync the endpoint to the new versions; the dropped table has no log any more
        synced = endpoint.sync(SCHEMA, [dv, part, widen, renamed, dropped])
        # NotRun: nothing left to sync for that table (already current); the data comparison below decides
        assert all(synced.get(t, ("missing",))[0] in ("Success", "NotRun") for t in (dv, part, widen, renamed)), synced
        seen = endpoint.tables(prefix)
        assert {dv, part, widen, renamed} <= set(seen) and dropped not in seen, seen

        def both(duck_sql, sql_endpoint_sql):
            want = tuple(conn.execute(duck_sql).fetchone())
            got = tuple(endpoint.query(sql_endpoint_sql)[0])
            assert got == want, f"SQL endpoint {got} != DuckLake {want}\n  {sql_endpoint_sql}"

        fq = lambda t: f"[{seen[t]}].[{t}]"  # noqa: E731
        both(f"SELECT count(*), sum(id), count(DISTINCT name), count(*) FILTER (name = 'updated') FROM {q(dv)}",
             f"SELECT COUNT_BIG(*), SUM(id), COUNT(DISTINCT name), SUM(CASE WHEN name = 'updated' THEN 1 ELSE 0 END) FROM {fq(dv)}")
        both(f"SELECT count(*) FROM {q(dv)} WHERE id % 7 = 0 AND id < 19900",
             f"SELECT COUNT_BIG(*) FROM {fq(dv)} WHERE id % 7 = 0 AND id < 19900")
        both(f"SELECT count(*), sum(amount), count(DISTINCT region) FROM {q(part)}",
             f"SELECT COUNT_BIG(*), SUM(CAST(amount AS BIGINT)), COUNT(DISTINCT region) FROM {fq(part)}")
        both(f"SELECT count(*), min(score), max(score), sum(id) FROM {q(widen)}",
             f"SELECT COUNT_BIG(*), MIN(score), MAX(score), SUM(id) FROM {fq(widen)}")
        # the held-back table still shows its last good export: old name, no NULLs
        assert endpoint.query(f"SELECT COUNT_BIG(*), SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) FROM {fq(renamed)}")[0] == (100, 0)
    finally:
        # leave nothing behind in the lakehouse: drop, expire, clean the files, remove the logs
        for t in (dv, part, widen, renamed, dropped):
            conn.execute(f"DROP TABLE IF EXISTS {q(t)}")
        conn.execute("CALL ducklake_expire_snapshots('test_lake', older_than => now())")
        conn.execute("CALL ducklake_cleanup_old_files('test_lake', cleanup_all => true)")
        print(f"cleanup: {conn.execute('SELECT * FROM delta_export()').fetchall()}")
