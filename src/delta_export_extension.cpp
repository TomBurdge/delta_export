#include "delta_export_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/prepared_statement.hpp"

#include <map>

namespace duckdb {

//===--------------------------------------------------------------------===//
// SQL Statements — kept close to the original export.sql for maintainability
//===--------------------------------------------------------------------===//

static constexpr const char *SQL_CREATE_EXPORT_SUMMARY = R"(
CREATE OR REPLACE TEMP TABLE export_summary (
    table_id BIGINT,
    schema_name VARCHAR,
    table_name VARCHAR,
    table_root VARCHAR,
    status VARCHAR,
    snapshot_id BIGINT,
    message VARCHAR
);
)";

static constexpr const char *SQL_INSERT_EXPORT_SUMMARY = R"(
INSERT INTO export_summary
WITH
data_root_config AS (
    SELECT value AS data_root FROM {META}ducklake_metadata WHERE key = 'data_path'
),
active_tables AS (
    SELECT
        t.table_id,
        t.table_name,
        s.schema_name,
        t.path AS table_path,
        s.path AS schema_path,
        -- DuckLake spec: schema path is relative to data_path, table path is relative to the
        -- schema path, unless path_is_relative is false (then the path is used as-is).
        CASE
            WHEN NOT COALESCE(t.path_is_relative, true) THEN rtrim(t.path, '/')
            ELSE
                CASE
                    WHEN NOT COALESCE(s.path_is_relative, true) THEN rtrim(s.path, '/')
                    ELSE rtrim((SELECT data_root FROM data_root_config), '/') ||
                        CASE
                            WHEN trim(s.path, '/') != '' THEN '/' || trim(s.path, '/')
                            ELSE ''
                        END
                END || '/' || trim(t.path, '/')
        END AS table_root
    FROM {META}ducklake_table t
    JOIN {META}ducklake_schema s USING(schema_id)
    WHERE t.end_snapshot IS NULL
),
current_snapshot AS (
    SELECT MAX(snapshot_id) AS snapshot_id FROM {META}ducklake_snapshot
),
table_last_modified AS (
    SELECT
        t.*,
        COALESCE(
            (SELECT MAX(sc.snapshot_id)
             FROM {META}ducklake_snapshot_changes sc
             WHERE regexp_matches(sc.changes_made, '[:,]' || t.table_id || '([^0-9]|$)')
            ),
            (SELECT cs.snapshot_id
             FROM current_snapshot cs
             WHERE EXISTS (
                 SELECT 1 FROM {META}ducklake_data_file df
                 WHERE df.table_id = t.table_id
                   AND df.end_snapshot IS NULL
             )
            )
        ) AS last_modified_snapshot,
        (SELECT COUNT(*) FROM {META}ducklake_data_file df
         WHERE df.table_id = t.table_id
           AND df.end_snapshot IS NULL
        ) AS file_count
    FROM active_tables t
)
SELECT
    table_id,
    schema_name,
    table_name,
    table_root,
    CASE
        WHEN file_count = 0 THEN 'no_data_files'
        ELSE 'needs_export'
    END AS status,
    last_modified_snapshot AS snapshot_id,
    CASE
        WHEN file_count = 0 THEN 'Table has no data files (empty table)'
        ELSE 'Ready for export'
    END AS message
FROM table_last_modified;
)";

// Current columns of every table that will be exported, including nested child columns
// (parent_column IS NOT NULL). The Delta schema is assembled from these rows in C++.
static constexpr const char *SQL_GET_EXPORT_COLUMNS = R"(
SELECT
    c.table_id,
    c.column_id,
    c.column_order,
    c.column_name,
    c.column_type,
    COALESCE(c.nulls_allowed, true) AS nulls_allowed,
    c.parent_column
FROM {META}ducklake_column c
JOIN export_summary es ON es.table_id = c.table_id
WHERE es.status = 'needs_export'
  AND c.end_snapshot IS NULL
ORDER BY c.table_id, c.column_order, c.column_id;
)";

static constexpr const char *SQL_CREATE_CHECKPOINT_PARQUET = R"(
CREATE OR REPLACE TEMP TABLE temp_checkpoint_parquet AS
WITH
tables_to_export AS (
    SELECT es.*
    FROM export_summary es
    WHERE es.status = 'needs_export'
      AND EXISTS (
          SELECT 1 FROM {META}ducklake_data_file df
          WHERE df.table_id = es.table_id
            AND df.end_snapshot IS NULL
      )
),
table_schemas AS (
    SELECT
        te.table_id,
        te.schema_name,
        te.table_name,
        te.snapshot_id,
        te.table_root,
        tsj.schema_string
    FROM tables_to_export te
    JOIN table_schema_json tsj USING (table_id)
),
file_column_stats_agg AS (
    SELECT
        ts.table_id,
        df.data_file_id,
        c.column_name,
        ANY_VALUE(c.column_type) AS column_type,
        MAX(fcs.value_count) AS value_count,
        MIN(fcs.min_value) AS min_value,
        MAX(fcs.max_value) AS max_value,
        MAX(fcs.null_count) AS null_count
    FROM table_schemas ts
    JOIN {META}ducklake_data_file df ON ts.table_id = df.table_id
    LEFT JOIN {META}ducklake_file_column_stats fcs ON df.data_file_id = fcs.data_file_id
    LEFT JOIN {META}ducklake_column c ON fcs.column_id = c.column_id
    WHERE df.end_snapshot IS NULL
      AND c.column_id IS NOT NULL
      AND c.end_snapshot IS NULL
      AND c.parent_column IS NULL
    GROUP BY ts.table_id, df.data_file_id, c.column_name
),
file_column_stats_transformed AS (
    SELECT
        fca.table_id,
        fca.data_file_id,
        fca.column_name,
        fca.column_type,
        fca.value_count,
        fca.null_count,
        CASE
            WHEN fca.min_value IS NULL THEN NULL
            WHEN contains(lower(fca.column_type), 'timestamp') THEN
                regexp_replace(
                    regexp_replace(replace(fca.min_value, ' ', 'T'), '[+-]\d{2}(?::\d{2})?$', ''),
                    '^([^.]+)$', '\1.000'
                ) || 'Z'
            WHEN contains(lower(fca.column_type), 'date') THEN fca.min_value
            WHEN contains(lower(fca.column_type), 'bool') THEN CAST(lower(fca.min_value) IN ('true', 't', '1', 'yes') AS VARCHAR)
            WHEN contains(lower(fca.column_type), 'int') OR contains(lower(fca.column_type), 'float')
                 OR contains(lower(fca.column_type), 'double') OR contains(lower(fca.column_type), 'decimal') THEN
                CASE WHEN contains(fca.min_value, '.') OR contains(lower(fca.min_value), 'e')
                     THEN CAST(TRY_CAST(fca.min_value AS DOUBLE) AS VARCHAR)
                     ELSE CAST(TRY_CAST(fca.min_value AS BIGINT) AS VARCHAR)
                END
            ELSE fca.min_value
        END AS transformed_min,
        CASE
            WHEN fca.max_value IS NULL THEN NULL
            WHEN contains(lower(fca.column_type), 'timestamp') THEN
                regexp_replace(
                    regexp_replace(replace(fca.max_value, ' ', 'T'), '[+-]\d{2}(?::\d{2})?$', ''),
                    '^([^.]+)$', '\1.000'
                ) || 'Z'
            WHEN contains(lower(fca.column_type), 'date') THEN fca.max_value
            WHEN contains(lower(fca.column_type), 'bool') THEN CAST(lower(fca.max_value) IN ('true', 't', '1', 'yes') AS VARCHAR)
            WHEN contains(lower(fca.column_type), 'int') OR contains(lower(fca.column_type), 'float')
                 OR contains(lower(fca.column_type), 'double') OR contains(lower(fca.column_type), 'decimal') THEN
                CASE WHEN contains(fca.max_value, '.') OR contains(lower(fca.max_value), 'e')
                     THEN CAST(TRY_CAST(fca.max_value AS DOUBLE) AS VARCHAR)
                     ELSE CAST(TRY_CAST(fca.max_value AS BIGINT) AS VARCHAR)
                END
            ELSE fca.max_value
        END AS transformed_max
    FROM file_column_stats_agg fca
),
file_metadata AS (
    SELECT
        ts.table_id,
        ts.schema_name,
        ts.table_name,
        ts.snapshot_id,
        ts.table_root,
        ts.schema_string,
        df.data_file_id,
        df.path AS file_path,
        df.path_is_relative,
        df.file_size_bytes,
        df.record_count AS num_records,
        COALESCE(map_from_entries(list({
            'key': fct.column_name,
            'value': fct.transformed_min
        } ORDER BY fct.column_name) FILTER (WHERE fct.column_name IS NOT NULL AND fct.transformed_min IS NOT NULL)), MAP{}::MAP(VARCHAR, VARCHAR)) AS min_values,
        COALESCE(map_from_entries(list({
            'key': fct.column_name,
            'value': fct.transformed_max
        } ORDER BY fct.column_name) FILTER (WHERE fct.column_name IS NOT NULL AND fct.transformed_max IS NOT NULL)), MAP{}::MAP(VARCHAR, VARCHAR)) AS max_values,
        COALESCE(map_from_entries(list({
            'key': fct.column_name,
            'value': fct.null_count
        } ORDER BY fct.column_name) FILTER (WHERE fct.column_name IS NOT NULL AND fct.null_count IS NOT NULL)), MAP{}::MAP(VARCHAR, BIGINT)) AS null_count
    FROM table_schemas ts
    JOIN {META}ducklake_data_file df ON ts.table_id = df.table_id
    LEFT JOIN file_column_stats_transformed fct ON df.data_file_id = fct.data_file_id
    WHERE df.end_snapshot IS NULL
    GROUP BY ts.table_id, ts.schema_name, ts.table_name, ts.snapshot_id,
             ts.table_root, ts.schema_string, df.data_file_id, df.path, df.path_is_relative,
             df.file_size_bytes, df.record_count
),
table_aggregates AS (
    SELECT
        table_id,
        schema_name,
        table_name,
        snapshot_id,
        table_root,
        schema_string,
        COUNT(*) AS num_files,
        SUM(num_records) AS total_rows,
        SUM(file_size_bytes) AS total_bytes,
        list({
            'path': CASE WHEN COALESCE(path_is_relative, true) THEN ltrim(file_path, '/') ELSE file_path END,
            'partitionValues': MAP{}::MAP(VARCHAR, VARCHAR),
            'size': file_size_bytes,
            'modificationTime': epoch_ms(now()),
            'dataChange': true,
            'stats': COALESCE(to_json({
                'numRecords': COALESCE(num_records, 0),
                'minValues': COALESCE(min_values, MAP{}::MAP(VARCHAR, VARCHAR)),
                'maxValues': COALESCE(max_values, MAP{}::MAP(VARCHAR, VARCHAR)),
                'nullCount': COALESCE(null_count, MAP{}::MAP(VARCHAR, BIGINT))
            }), '{"numRecords":0}'),
            'tags': MAP{}::MAP(VARCHAR, VARCHAR)
        }::STRUCT(
            path VARCHAR,
            partitionValues MAP(VARCHAR, VARCHAR),
            size BIGINT,
            modificationTime BIGINT,
            dataChange BOOLEAN,
            stats VARCHAR,
            tags MAP(VARCHAR, VARCHAR)
        )) AS add_entries
    FROM file_metadata
    GROUP BY table_id, schema_name, table_name, snapshot_id, table_root, schema_string
),
checkpoint_data AS (
    SELECT
        ta.*,
        now() AS now_ts,
        epoch_ms(now()) AS now_ms,
        uuid()::VARCHAR AS txn_id,
        (substring(md5(ta.table_id::VARCHAR || '-metadata'), 1, 8) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 9, 4) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 13, 4) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 17, 4) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 21, 12)) AS meta_id
    FROM table_aggregates ta
),
checkpoint_parquet_data AS (
    SELECT
        cd.table_id,
        cd.schema_name,
        cd.table_name,
        cd.snapshot_id,
        cd.table_root,
        cd.meta_id,
        cd.now_ms,
        cd.txn_id,
        cd.schema_string,
        cd.num_files,
        cd.total_rows,
        cd.total_bytes,
        {'minReaderVersion': 1, 'minWriterVersion': 2} AS protocol,
        NULL AS metaData,
        NULL AS add,
        NULL::STRUCT(
            path VARCHAR,
            deletionTimestamp BIGINT,
            dataChange BOOLEAN
        ) AS remove,
        NULL::STRUCT(
            timestamp TIMESTAMP,
            operation VARCHAR,
            operationParameters MAP(VARCHAR, VARCHAR),
            isolationLevel VARCHAR,
            isBlindAppend BOOLEAN,
            operationMetrics MAP(VARCHAR, VARCHAR),
            engineInfo VARCHAR,
            txnId VARCHAR
        ) AS commitInfo,
        1 AS row_order
    FROM checkpoint_data cd
    UNION ALL
    SELECT
        cd.table_id,
        cd.schema_name,
        cd.table_name,
        cd.snapshot_id,
        cd.table_root,
        cd.meta_id,
        cd.now_ms,
        cd.txn_id,
        cd.schema_string,
        cd.num_files,
        cd.total_rows,
        cd.total_bytes,
        NULL AS protocol,
        {
            'id': cd.meta_id,
            'name': cd.table_name,
            'format': {'provider': 'parquet', 'options': MAP{}::MAP(VARCHAR, VARCHAR)}::STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)),
            'schemaString': cd.schema_string,
            'partitionColumns': []::VARCHAR[],
            'createdTime': cd.now_ms,
            'configuration': MAP{}::MAP(VARCHAR, VARCHAR)
        }::STRUCT(
            id VARCHAR,
            name VARCHAR,
            format STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)),
            schemaString VARCHAR,
            partitionColumns VARCHAR[],
            createdTime BIGINT,
            configuration MAP(VARCHAR, VARCHAR)
        ) AS metaData,
        NULL AS add,
        NULL::STRUCT(
            path VARCHAR,
            deletionTimestamp BIGINT,
            dataChange BOOLEAN
        ) AS remove,
        NULL::STRUCT(
            timestamp TIMESTAMP,
            operation VARCHAR,
            operationParameters MAP(VARCHAR, VARCHAR),
            isolationLevel VARCHAR,
            isBlindAppend BOOLEAN,
            operationMetrics MAP(VARCHAR, VARCHAR),
            engineInfo VARCHAR,
            txnId VARCHAR
        ) AS commitInfo,
        2 AS row_order
    FROM checkpoint_data cd
    UNION ALL
    SELECT
        cd.table_id,
        cd.schema_name,
        cd.table_name,
        cd.snapshot_id,
        cd.table_root,
        cd.meta_id,
        cd.now_ms,
        cd.txn_id,
        cd.schema_string,
        cd.num_files,
        cd.total_rows,
        cd.total_bytes,
        NULL AS protocol,
        NULL AS metaData,
        unnest(cd.add_entries) AS add,
        NULL::STRUCT(
            path VARCHAR,
            deletionTimestamp BIGINT,
            dataChange BOOLEAN
        ) AS remove,
        NULL::STRUCT(
            timestamp TIMESTAMP,
            operation VARCHAR,
            operationParameters MAP(VARCHAR, VARCHAR),
            isolationLevel VARCHAR,
            isBlindAppend BOOLEAN,
            operationMetrics MAP(VARCHAR, VARCHAR),
            engineInfo VARCHAR,
            txnId VARCHAR
        ) AS commitInfo,
        3 AS row_order
    FROM checkpoint_data cd
)
SELECT * FROM checkpoint_parquet_data;
)";

static constexpr const char *SQL_CREATE_CHECKPOINT_JSON = R"(
CREATE OR REPLACE TEMP TABLE temp_checkpoint_json AS
SELECT DISTINCT
    p.table_id,
    p.table_root,
    p.snapshot_id,
    p.num_files,
    to_json({
        'commitInfo': {
            'timestamp': p.now_ms,
            'operation': 'CONVERT',
            'operationParameters': {
                'convertedFrom': 'DuckLake',
                'duckLakeSnapshotId': p.snapshot_id::VARCHAR,
                'partitionBy': '[]'
            },
            'isolationLevel': 'Serializable',
            'isBlindAppend': false,
            'operationMetrics': {
                'numFiles': p.num_files::VARCHAR,
                'numOutputRows': p.total_rows::VARCHAR,
                'numOutputBytes': p.total_bytes::VARCHAR
            },
            'engineInfo': 'DuckLake-Delta-Exporter/1.0.0',
            'txnId': p.txn_id
        }
    }) || chr(10) ||
    to_json({
        'metaData': {
            'id': p.meta_id,
            'name': p.table_name,
            'format': {'provider': 'parquet', 'options': MAP{}},
            'schemaString': p.schema_string::VARCHAR,
            'partitionColumns': [],
            'createdTime': p.now_ms,
            'configuration': MAP{}
        }
    }) || chr(10) ||
    to_json({
        'protocol': {'minReaderVersion': 1, 'minWriterVersion': 2}
    }) AS content
FROM temp_checkpoint_parquet p
WHERE p.row_order = 1;
)";

static constexpr const char *SQL_CREATE_LAST_CHECKPOINT = R"(
CREATE OR REPLACE TEMP TABLE temp_last_checkpoint AS
SELECT
    table_id,
    table_root,
    snapshot_id,
    '{"version":' || snapshot_id || ',"size":' || (2 + num_files) || '}' AS content
FROM temp_checkpoint_parquet
GROUP BY table_id, table_root, snapshot_id, num_files;
)";

static constexpr const char *SQL_CREATE_EXPORT_PATHS = R"(
CREATE OR REPLACE TEMP TABLE numbered_exports AS
SELECT
    table_id,
    table_root,
    snapshot_id,
    ROW_NUMBER() OVER (ORDER BY table_id) AS export_order
FROM (SELECT DISTINCT table_id, table_root, snapshot_id FROM temp_checkpoint_parquet);

CREATE OR REPLACE TEMP TABLE export_paths AS
SELECT
    ne.export_order,
    ne.table_id,
    ne.table_root,
    ne.snapshot_id,
    paths.checkpoint_file,
    paths.json_file,
    paths.last_checkpoint_file,
    paths.delta_log_path
FROM numbered_exports ne,
LATERAL (
    SELECT
        ne.table_root || '/_delta_log/' || lpad(ne.snapshot_id::VARCHAR, 20, '0') || '.checkpoint.parquet' AS checkpoint_file,
        ne.table_root || '/_delta_log/' || lpad(ne.snapshot_id::VARCHAR, 20, '0') || '.json' AS json_file,
        ne.table_root || '/_delta_log/_last_checkpoint' AS last_checkpoint_file,
        ne.table_root || '/_delta_log' AS delta_log_path
) AS paths;
)";

static constexpr const char *SQL_GET_LOCAL_DIRS = R"(
SELECT DISTINCT delta_log_path
FROM export_paths
WHERE table_root NOT LIKE 'abfss://%'
  AND table_root NOT LIKE 's3://%'
  AND table_root NOT LIKE 'gs://%'
  AND table_root NOT LIKE 'az://%'
  AND table_root NOT LIKE 'http://%'
  AND table_root NOT LIKE 'https://%';
)";

static constexpr const char *SQL_GET_EXPORTS = R"(
SELECT
    export_order,
    table_id,
    table_root,
    snapshot_id,
    checkpoint_file,
    json_file,
    last_checkpoint_file
FROM export_paths
ORDER BY export_order;
)";

static constexpr const char *SQL_GET_EXPORT_SUMMARY = R"(
SELECT
    schema_name || '.' || table_name AS table_name,
    status,
    snapshot_id AS data_snapshot,
    CASE
        WHEN status = 'no_data_files' THEN 'Table has 0 parquet files (empty)'
        WHEN status = 'needs_export' THEN 'Data snapshot ' || snapshot_id || ' exported to Delta'
    END AS explanation,
    message
FROM export_summary
ORDER BY
    CASE status
        WHEN 'needs_export' THEN 1
        WHEN 'no_data_files' THEN 2
    END,
    schema_name, table_name;
)";

//===--------------------------------------------------------------------===//
// Helper: Locate the DuckLake metadata catalog
//===--------------------------------------------------------------------===//
struct MetadataLocation {
	//! Catalog holding the ducklake_* tables; empty means "resolve through the search path"
	string catalog;
	//! Schema holding the ducklake_* tables (METADATA_SCHEMA attach option); empty means the default schema
	string schema;
};

// DuckLake (on DuckDB >= 1.5.3) attaches its metadata catalog HIDDEN unless METADATA_CATALOG is given, so
// duckdb_databases() no longer lists it. DatabaseManager::GetDatabases includes hidden entries (it is what
// duckdb_databases() itself calls), and a hidden catalog still resolves by name in queries.
static MetadataLocation ResolveMetadataLocation(ClientContext &context) {
	auto &db_manager = DatabaseManager::Get(context);
	string default_db;
	try {
		default_db = DatabaseManager::GetDefaultDatabase(context);
	} catch (...) {
		// no default database set; fall back to the first attached lake
	}

	MetadataLocation default_lake, first_lake, prefixed;
	bool has_default_lake = false;
	bool has_first_lake = false;
	for (auto &db : db_manager.GetDatabases(context)) {
		if (db->IsSystem() || db->IsTemporary()) {
			continue;
		}
		auto &name = db->GetName();
		if (db->GetCatalog().GetCatalogType() == "ducklake") {
			MetadataLocation loc;
			loc.catalog = "__ducklake_metadata_" + name;
			// ATTACH option keys are lowercased by the parser; DuckLake leaves them in the map
			auto &options = db->GetAttachOptions();
			auto catalog_opt = options.find("metadata_catalog");
			if (catalog_opt != options.end() && !catalog_opt->second.IsNull()) {
				loc.catalog = catalog_opt->second.ToString();
			}
			auto schema_opt = options.find("metadata_schema");
			if (schema_opt != options.end() && !schema_opt->second.IsNull()) {
				loc.schema = schema_opt->second.ToString();
			}
			if (!has_first_lake) {
				first_lake = loc;
				has_first_lake = true;
			}
			if (name == default_db) {
				default_lake = loc;
				has_default_lake = true;
			}
		} else if (prefixed.catalog.empty() && StringUtil::StartsWith(name, "__ducklake_metadata_")) {
			// Older DuckLake (visible metadata catalog) or a manually attached metadata database
			prefixed.catalog = name;
		}
	}
	if (has_default_lake) {
		return default_lake; // USE lake -> that lake
	}
	if (has_first_lake) {
		return first_lake; // no USE (dbt) -> first attached lake
	}
	return prefixed; // may be empty: metadata database attached directly, unqualified names
}

//===--------------------------------------------------------------------===//
// Helper: Qualify ducklake_* table names with the metadata catalog/schema prefix
//===--------------------------------------------------------------------===//
static string QualifyMetadataTables(const string &sql, const MetadataLocation &meta) {
	string prefix;
	if (!meta.catalog.empty()) {
		prefix += "\"" + meta.catalog + "\".";
	}
	if (!meta.schema.empty()) {
		prefix += "\"" + meta.schema + "\".";
	}
	return StringUtil::Replace(sql, "{META}", prefix);
}

//===--------------------------------------------------------------------===//
// Helper: Execute SQL and throw on error
//===--------------------------------------------------------------------===//
static unique_ptr<MaterializedQueryResult> ExecuteSQL(Connection &conn, const string &sql) {
	auto result = conn.Query(sql);
	if (result->HasError()) {
		throw InvalidInputException("Delta export failed: %s\nSQL: %s", result->GetError(), sql.substr(0, 200));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Helper: Build the Delta Lake schemaString of each exported table
//===--------------------------------------------------------------------===//
struct DuckLakeColumn {
	int64_t column_id = 0;
	string name;
	string type;
	bool nullable = true;
	bool has_parent = false;
	int64_t parent_id = 0;
	//! Indexes (into the table's column vector) of the nested child columns, in column_order
	vector<idx_t> children;
};

// DuckLake stores types as internal names (int32, varchar, ...). Delta has no equivalent for uuid, time,
// timetz, interval, int128 and uint64; those are exported as 'string', which is lossy: Delta readers will
// not decode the parquet values of such columns.
static string MapPrimitiveType(const string &column_type) {
	auto t = StringUtil::Lower(column_type);
	if (t == "int8" || t == "tinyint") {
		return "byte";
	}
	if (t == "int16" || t == "smallint") {
		return "short";
	}
	if (t == "int32" || t == "integer" || t == "int") {
		return "integer";
	}
	if (t == "int64" || t == "bigint") {
		return "long";
	}
	if (t == "uint8" || t == "utinyint") {
		return "short";
	}
	if (t == "uint16" || t == "usmallint") {
		return "integer";
	}
	if (t == "uint32" || t == "uinteger") {
		return "long";
	}
	if (t == "uint64" || t == "ubigint" || t == "int128" || t == "hugeint" || t == "uint128" || t == "uhugeint") {
		return "string";
	}
	if (t == "float32" || t == "float") {
		return "float";
	}
	if (t == "float64" || t == "double") {
		return "double";
	}
	if (t == "boolean") {
		return "boolean";
	}
	if (t == "date") {
		return "date";
	}
	if (t == "blob") {
		return "binary";
	}
	if (t == "varchar") {
		return "string";
	}
	if (StringUtil::StartsWith(t, "decimal")) {
		return StringUtil::Replace(t, " ", "");
	}
	if (StringUtil::Contains(t, "timestamp")) {
		return "timestamp";
	}
	// json is stored as a string in parquet; uuid, time, timetz and interval have no Delta equivalent
	return "string";
}

static string JsonEscape(const string &input) {
	static const char *HEX = "0123456789abcdef";
	string out;
	out.reserve(input.size() + 2);
	for (auto ch : input) {
		auto c = static_cast<unsigned char>(ch);
		switch (c) {
		case '"':
			out += "\\\"";
			break;
		case '\\':
			out += "\\\\";
			break;
		case '\n':
			out += "\\n";
			break;
		case '\r':
			out += "\\r";
			break;
		case '\t':
			out += "\\t";
			break;
		default:
			if (c < 0x20) {
				out += "\\u00";
				out += HEX[c >> 4];
				out += HEX[c & 0xF];
			} else {
				out += ch;
			}
		}
	}
	return out;
}

static string BuildDeltaType(const vector<DuckLakeColumn> &columns, idx_t idx);

static string BuildDeltaField(const vector<DuckLakeColumn> &columns, idx_t idx) {
	auto &col = columns[idx];
	return "{\"name\":\"" + JsonEscape(col.name) + "\",\"type\":" + BuildDeltaType(columns, idx) +
	       ",\"nullable\":" + (col.nullable ? "true" : "false") + ",\"metadata\":{}}";
}

static string BuildDeltaType(const vector<DuckLakeColumn> &columns, idx_t idx) {
	auto &col = columns[idx];
	auto t = StringUtil::Lower(col.type);
	if (t == "struct") {
		string fields;
		for (auto child : col.children) {
			if (!fields.empty()) {
				fields += ",";
			}
			fields += BuildDeltaField(columns, child);
		}
		return "{\"type\":\"struct\",\"fields\":[" + fields + "]}";
	}
	if (t == "list") {
		if (col.children.empty()) {
			throw InvalidInputException("Delta export failed: list column \"%s\" has no element column", col.name);
		}
		auto &element = columns[col.children[0]];
		return "{\"type\":\"array\",\"elementType\":" + BuildDeltaType(columns, col.children[0]) +
		       ",\"containsNull\":" + (element.nullable ? "true" : "false") + "}";
	}
	if (t == "map") {
		idx_t key_idx = DConstants::INVALID_INDEX;
		idx_t value_idx = DConstants::INVALID_INDEX;
		for (auto child : col.children) {
			if (columns[child].name == "key" && key_idx == DConstants::INVALID_INDEX) {
				key_idx = child;
			} else if (columns[child].name == "value" && value_idx == DConstants::INVALID_INDEX) {
				value_idx = child;
			}
		}
		if (key_idx == DConstants::INVALID_INDEX || value_idx == DConstants::INVALID_INDEX) {
			if (col.children.size() != 2) {
				throw InvalidInputException("Delta export failed: map column \"%s\" has no key/value columns",
				                            col.name);
			}
			key_idx = col.children[0];
			value_idx = col.children[1];
		}
		auto &value = columns[value_idx];
		return "{\"type\":\"map\",\"keyType\":" + BuildDeltaType(columns, key_idx) +
		       ",\"valueType\":" + BuildDeltaType(columns, value_idx) +
		       ",\"valueContainsNull\":" + (value.nullable ? "true" : "false") + "}";
	}
	return "\"" + MapPrimitiveType(t) + "\"";
}

static string BuildDeltaSchema(vector<DuckLakeColumn> &columns) {
	// Link nested child columns to their parents (columns arrive in column_order)
	std::map<int64_t, idx_t> index_of;
	for (idx_t i = 0; i < columns.size(); i++) {
		index_of[columns[i].column_id] = i;
	}
	for (idx_t i = 0; i < columns.size(); i++) {
		if (!columns[i].has_parent) {
			continue;
		}
		auto parent = index_of.find(columns[i].parent_id);
		if (parent == index_of.end()) {
			throw InvalidInputException("Delta export failed: column \"%s\" references unknown parent column %s",
			                            columns[i].name, std::to_string(columns[i].parent_id));
		}
		columns[parent->second].children.push_back(i);
	}
	string fields;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (columns[i].has_parent) {
			continue;
		}
		if (!fields.empty()) {
			fields += ",";
		}
		fields += BuildDeltaField(columns, i);
	}
	return "{\"type\":\"struct\",\"fields\":[" + fields + "]}";
}

// Fills TEMP table table_schema_json(table_id, schema_string). TEMP tables are per connection, so this must
// run on the same Connection as the rest of the export.
static void BuildTableSchemas(Connection &conn, const MetadataLocation &meta) {
	ExecuteSQL(conn, "CREATE OR REPLACE TEMP TABLE table_schema_json (table_id BIGINT, schema_string VARCHAR);");
	auto columns_result = ExecuteSQL(conn, QualifyMetadataTables(SQL_GET_EXPORT_COLUMNS, meta));

	std::map<int64_t, vector<DuckLakeColumn>> table_columns;
	for (idx_t i = 0; i < columns_result->RowCount(); i++) {
		auto table_id = columns_result->GetValue(0, i).GetValue<int64_t>();
		DuckLakeColumn col;
		col.column_id = columns_result->GetValue(1, i).GetValue<int64_t>();
		col.name = columns_result->GetValue(3, i).ToString();
		col.type = columns_result->GetValue(4, i).ToString();
		col.nullable = columns_result->GetValue(5, i).GetValue<bool>();
		auto parent = columns_result->GetValue(6, i);
		if (!parent.IsNull()) {
			col.has_parent = true;
			col.parent_id = parent.GetValue<int64_t>();
		}
		table_columns[table_id].push_back(std::move(col));
	}

	auto insert = conn.Prepare("INSERT INTO table_schema_json VALUES ($1, $2)");
	if (insert->HasError()) {
		throw InvalidInputException("Delta export failed: %s", insert->GetError());
	}
	for (auto &entry : table_columns) {
		auto schema_string = BuildDeltaSchema(entry.second);
		auto result = insert->Execute(Value::BIGINT(entry.first), Value(schema_string));
		if (result->HasError()) {
			throw InvalidInputException("Delta export failed: %s", result->GetError());
		}
	}
}

//===--------------------------------------------------------------------===//
// Table Function: delta_export
//===--------------------------------------------------------------------===//

struct DeltaExportBindData : public TableFunctionData {};

struct DeltaExportGlobalState : public GlobalTableFunctionState {
	bool finished = false;
	vector<vector<Value>> rows;
	idx_t current_row = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> DeltaExportBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("data_snapshot");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("explanation");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("message");
	return_types.emplace_back(LogicalType::VARCHAR);

	return make_uniq<DeltaExportBindData>();
}

static unique_ptr<GlobalTableFunctionState> DeltaExportGlobalInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	return make_uniq<DeltaExportGlobalState>();
}

static void DeltaExportScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<DeltaExportGlobalState>();

	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	// If we already have rows, emit them
	if (!state.rows.empty()) {
		idx_t count = 0;
		while (state.current_row < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
			auto &row = state.rows[state.current_row];
			for (idx_t col = 0; col < row.size(); col++) {
				output.data[col].SetValue(count, row[col]);
			}
			count++;
			state.current_row++;
		}
		output.SetCardinality(count);
		if (state.current_row >= state.rows.size()) {
			state.finished = true;
		}
		return;
	}

	// ── Run export on the current database ──
	// Use a separate Connection to avoid deadlocking context.Query() inside a scan
	Connection conn(*context.db);
	// Locate the DuckLake metadata catalog from the main context — works with USE, dbt (no USE), a hidden
	// metadata catalog (DuckLake on DuckDB >= 1.5.3), METADATA_CATALOG / METADATA_SCHEMA, or a direct attach
	auto meta = ResolveMetadataLocation(context);

	// Step 1: Build export summary (reads ducklake_* tables)
	ExecuteSQL(conn, SQL_CREATE_EXPORT_SUMMARY);
	ExecuteSQL(conn, QualifyMetadataTables(SQL_INSERT_EXPORT_SUMMARY, meta));

	// Step 2: Build the Delta schema of every exported table (nested types, nulls_allowed)
	BuildTableSchemas(conn, meta);

	// Step 3: Generate checkpoint parquet data (reads ducklake_* tables)
	ExecuteSQL(conn, QualifyMetadataTables(SQL_CREATE_CHECKPOINT_PARQUET, meta));

	// Step 4: Generate JSON checkpoint
	ExecuteSQL(conn, SQL_CREATE_CHECKPOINT_JSON);

	// Step 5: Generate last checkpoint
	ExecuteSQL(conn, SQL_CREATE_LAST_CHECKPOINT);

	// Step 6: Build export paths
	ExecuteSQL(conn, SQL_CREATE_EXPORT_PATHS);

	// Step 7: Create _delta_log directories for local paths
	auto dirs_result = ExecuteSQL(conn, SQL_GET_LOCAL_DIRS);
	auto &fs = context.db->GetFileSystem();
	for (idx_t i = 0; i < dirs_result->RowCount(); i++) {
		string dir_path = dirs_result->GetValue(0, i).ToString();
		std::replace(dir_path.begin(), dir_path.end(), '\\', '/');
		fs.CreateDirectoriesRecursive(dir_path);
	}

	// Step 8: Export loop — unlimited tables
	auto exports_result = ExecuteSQL(conn, SQL_GET_EXPORTS);

	for (idx_t i = 0; i < exports_result->RowCount(); i++) {
		int64_t table_id = exports_result->GetValue(1, i).GetValue<int64_t>();
		int64_t snapshot_id = exports_result->GetValue(3, i).GetValue<int64_t>();
		string checkpoint_file = exports_result->GetValue(4, i).ToString();
		string json_file = exports_result->GetValue(5, i).ToString();
		string last_checkpoint_file = exports_result->GetValue(6, i).ToString();

		string tid = std::to_string(table_id);
		string sid = std::to_string(snapshot_id);

		string copy_parquet = "COPY (SELECT protocol, metaData, add, remove, commitInfo "
		                      "FROM temp_checkpoint_parquet "
		                      "WHERE table_id = " +
		                      tid + " AND snapshot_id = " + sid +
		                      " ORDER BY row_order) "
		                      "TO '" +
		                      checkpoint_file + "' (FORMAT PARQUET);";
		ExecuteSQL(conn, copy_parquet);

		string copy_json = "COPY (SELECT content FROM temp_checkpoint_json "
		                   "WHERE table_id = " +
		                   tid + " AND snapshot_id = " + sid +
		                   ") "
		                   "TO '" +
		                   json_file + "' (FORMAT CSV, HEADER false, QUOTE '');";
		ExecuteSQL(conn, copy_json);

		string copy_last = "COPY (SELECT content FROM temp_last_checkpoint "
		                   "WHERE table_id = " +
		                   tid + " AND snapshot_id = " + sid +
		                   ") "
		                   "TO '" +
		                   last_checkpoint_file + "' (FORMAT CSV, HEADER false, QUOTE '');";
		ExecuteSQL(conn, copy_last);
	}

	// Step 9: Collect and return results
	auto summary_result = ExecuteSQL(conn, SQL_GET_EXPORT_SUMMARY);

	for (idx_t i = 0; i < summary_result->RowCount(); i++) {
		vector<Value> row;
		row.push_back(summary_result->GetValue(0, i));
		row.push_back(summary_result->GetValue(1, i));
		row.push_back(summary_result->GetValue(2, i));
		row.push_back(summary_result->GetValue(3, i));
		row.push_back(summary_result->GetValue(4, i));
		state.rows.push_back(std::move(row));
	}

	// Emit first batch of rows
	idx_t count = 0;
	while (state.current_row < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.current_row];
		for (idx_t col = 0; col < row.size(); col++) {
			output.data[col].SetValue(count, row[col]);
		}
		count++;
		state.current_row++;
	}
	output.SetCardinality(count);

	if (state.current_row >= state.rows.size()) {
		state.finished = true;
	}
}

//===--------------------------------------------------------------------===//
// Extension loading
//===--------------------------------------------------------------------===//

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction func("delta_export", {}, DeltaExportScan, DeltaExportBind);
	func.init_global = DeltaExportGlobalInit;
	loader.RegisterFunction(func);
}

void DeltaExportExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DeltaExportExtension::Name() {
	return "delta_export";
}

std::string DeltaExportExtension::Version() const {
#ifdef EXT_VERSION_DELTA_EXPORT
	return EXT_VERSION_DELTA_EXPORT;
#else
	return "1.0.0";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(delta_export, loader) {
	duckdb::LoadInternal(loader);
}
}
