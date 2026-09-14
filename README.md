# Delta Export — DuckDB Extension

Export DuckLake catalog tables to Delta Lake format, enabling consumption by Delta Lake readers such as Power BI DirectLake, Apache Spark, Databricks, and other tools that understand the Delta protocol.

## Install

```sql
INSTALL delta_export FROM community;
LOAD delta_export;
```

## Usage

```sql
ATTACH 'metadata.ducklake' AS my_catalog;
USE my_catalog;
CALL delta_export();
```

The function returns a summary table showing the export status of each table:

| table_name | status | data_snapshot | explanation | message |
|---|---|---|---|---|
| main.sales | needs_export | 5 | Data snapshot 5 exported to Delta | Ready for export |
| main.products | no_data_files | 3 | Table has 0 parquet files (empty) | Table has no data files (empty table) |

### Status values

- **needs_export** — data was exported to Delta format
- **no_data_files** — table has no parquet files (empty table, skipped)

### Python

```python
import duckdb
con = duckdb.connect()
con.sql("""
    INSTALL delta_export FROM community;
    LOAD delta_export;
    ATTACH '/tmp/metadata.ducklake' AS db;
    USE db;
    CALL delta_export();
""")
```

## Features

- **Metadata-only export** — writes Delta Lake metadata on top of existing DuckLake Parquet files, no data copying
- **Column statistics** — generates Delta checkpoint files with min/max/null_count stats
- **Type mappings** — maps DuckLake types to Delta Lake schema types, including nested `LIST`, `STRUCT` and `MAP` columns; `NOT NULL` columns are exported as non-nullable
- **Absolute paths** — honours DuckLake's `path_is_relative` flag on schemas, tables and data files (for example files registered with `ducklake_add_data_files` from outside the data path)
- **Any storage backend** — supports any storage DuckDB supports: local filesystem, S3, Azure (abfss://), GCS
- **Any metadata backend** — finds the DuckLake metadata catalog whether it is hidden (the default on DuckDB 1.5.3+), named with `METADATA_CATALOG`, or placed in a schema with `METADATA_SCHEMA`; with several lakes attached, the one selected with `USE` is exported

### Type mapping

| DuckLake type | Delta type |
|---|---|
| int8 / int16 / int32 / int64 | byte / short / integer / long |
| uint8 / uint16 / uint32 | short / integer / long |
| float32 / float64 | float / double |
| boolean, date, varchar, blob | boolean, date, string, binary |
| decimal(P,S) | decimal(P,S) |
| timestamp, timestamptz, timestamp_s/ms/ns | timestamp |
| list, struct, map | array, struct, map (recursive) |
| json | string |
| uuid, time, timetz, interval, uint64, int128 | string (**lossy** — Delta has no equivalent, readers will not decode these columns) |

## How it works

1. Reads DuckLake internal metadata (`ducklake_table`, `ducklake_data_file`, `ducklake_column`, etc.)
2. Generates Delta Lake checkpoint files (`.checkpoint.parquet`, `.json`, `_last_checkpoint`) in each table's `_delta_log/` directory
3. Other engines (Spark, Databricks, Fabric, Trino, etc.) can then read the same Parquet files via the Delta protocol

**Important**: Before running `delta_export`, make sure to flush inlined data and rewrite files with deletes:

```sql
CALL ducklake_flush_inlined_data('my_catalog');
CALL my_catalog.set_option('rewrite_delete_threshold', 0);
CALL ducklake_rewrite_data_files('my_catalog');
CALL delta_export();
```

## Real-world example

For a production example using delta_export with dbt, see https://github.com/djouallah/dbt

## Requirements

- DuckDB v1.5.x+ (built and tested against v1.5.5)


## License

MIT
