"""Live OneLake round trip: DuckLake data on abfss://, delta_export() writes _delta_log there.

Auth is GitHub Actions OIDC only (workload-identity federation, no client secret). Skipped
unless running in the onelake-tests CI job with ONELAKE_WAREHOUSE=<workspace-guid>/<lakehouse-guid>.
"""

import json
import os
import posixpath
import urllib.request

import duckdb
import pytest

OIDC_ENV = ("AZURE_CLIENT_ID", "AZURE_TENANT_ID", "ACTIONS_ID_TOKEN_REQUEST_URL")
pytestmark = pytest.mark.skipif(
    not os.environ.get("ONELAKE_WAREHOUSE"),
    reason="live OneLake test: runs only in the onelake-tests CI job",
)

SCHEMA = "delta_export_it"


def _github_oidc_assertion():
    url = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"]
    sep = "&" if "?" in url else "?"
    request = urllib.request.Request(
        f"{url}{sep}audience=api://AzureADTokenExchange",
        headers={"Authorization": f"Bearer {os.environ['ACTIONS_ID_TOKEN_REQUEST_TOKEN']}"},
    )
    with urllib.request.urlopen(request, timeout=15) as resp:
        return json.loads(resp.read().decode())["value"]


def _storage_token():
    from azure.identity import ClientAssertionCredential

    missing = [v for v in OIDC_ENV if not os.environ.get(v)]
    # The job sets ONELAKE_WAREHOUSE, so missing secrets / id-token permission must fail, not skip
    assert not missing, f"OIDC not configured, missing: {missing}"

    cred = ClientAssertionCredential(
        os.environ["AZURE_TENANT_ID"], os.environ["AZURE_CLIENT_ID"], _github_oidc_assertion
    )
    return cred.get_token("https://storage.azure.com/.default").token


@pytest.fixture
def onelake_env(extension_path, tmp_path):
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
    con.execute(f"CREATE SECRET onelake (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{_storage_token()}')")
    con.execute(f"ATTACH 'ducklake:{tmp_path / 'meta.ducklake'}' AS test_lake (DATA_PATH '{data_path}')")
    con.execute("USE test_lake")
    yield con
    con.close()


def test_export_to_onelake(onelake_env):
    conn = onelake_env
    table = f"t_{os.environ.get('GITHUB_RUN_ID', 'local')}_{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    conn.execute(f"CREATE TABLE {SCHEMA}.{table} (id BIGINT, name VARCHAR)")
    conn.execute(f"INSERT INTO {SCHEMA}.{table} SELECT i, 'row_' || i FROM range(100) t(i)")
    conn.execute(f"UPDATE {SCHEMA}.{table} SET name = 'updated' WHERE id < 10")
    # No rewrite: the UPDATE's deletes export as deletion vectors, so DV files land on OneLake too
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")

    rows = conn.execute("SELECT * FROM delta_export()").fetchall()
    print(f"delta_export: {rows}")
    assert [r[0] for r in rows] == [f"{SCHEMA}.{table}"]

    data_file = conn.execute(f"SELECT data_file FROM ducklake_list_files('test_lake', '{table}', schema => '{SCHEMA}')").fetchone()[0]
    table_root = posixpath.dirname(data_file)
    print(f"table root: {table_root}")
    assert table_root.startswith("abfss://")

    last_checkpoint = conn.execute(f"SELECT content FROM read_text('{table_root}/_delta_log/_last_checkpoint')").fetchone()[0]
    assert "version" in json.loads(last_checkpoint)

    expected = conn.execute(f"SELECT count(*), count(*) FILTER (name = 'updated') FROM {SCHEMA}.{table}").fetchone()
    actual = conn.execute(f"SELECT count(*), count(*) FILTER (name = 'updated') FROM delta_scan('{table_root}')").fetchone()
    assert actual == expected == (100, 10)
