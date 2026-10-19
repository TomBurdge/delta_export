-- DuckLake: a filtered query misses rows that a full scan returns (local filesystem, default inlining).
-- Minimised from the delta_export fuzz tests (seed 29) by test/repro/ducklake_filter_repro.py.
INSTALL ducklake;
ATTACH 'ducklake:/tmp/ducklake_filter_bug/meta.ducklake' AS lake (DATA_PATH '/tmp/ducklake_filter_bug/data');
USE lake;
SET TimeZone = 'UTC';
CREATE TABLE t2 ("id" BIGINT, "grp" INTEGER, "c7" DECIMAL(38,10), "c3" INTEGER);
INSERT INTO t2 ("id", "grp", "c7", "c3") SELECT 0 + i, ((0 + i) % 7)::INTEGER, CASE WHEN hash(0 + i, 160, 29) % 10 = 0 THEN NULL ELSE ((((hash(0 + i, 8079, 29) % 1000000000000000000)::HUGEINT - 500000000000000000)::DECIMAL(38,0) / 10000000000)::DECIMAL(38,10)) END, CASE WHEN hash(0 + i, 291, 29) % 10 = 0 THEN NULL ELSE (CASE hash(0 + i, 8210, 29) % 20 WHEN 0 THEN (-2147483648)::INTEGER WHEN 1 THEN (2147483647)::INTEGER ELSE ((hash(0 + i, 8210, 29) % 4294967296)::HUGEINT - 2147483648)::INTEGER END) END FROM range(5000) r(i);
MERGE INTO t2 AS tgt USING (SELECT i AS id FROM range(2515, 3741) r(i)) AS src ON tgt.id = src.id WHEN MATCHED THEN UPDATE SET "c7" = CASE WHEN hash(src.id, 422, 29) % 10 = 0 THEN NULL ELSE ((((hash(src.id, 8341, 29) % 1000000000000000000)::HUGEINT - 500000000000000000)::DECIMAL(38,0) / 10000000000)::DECIMAL(38,10)) END WHEN NOT MATCHED THEN INSERT (id, grp) VALUES (src.id, (src.id % 7)::INTEGER);
ALTER TABLE t2 ADD COLUMN "c2" USMALLINT;
MERGE INTO t2 AS tgt USING (SELECT i AS id FROM range(3338, 6098) r(i)) AS src ON tgt.id = src.id WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT (id, grp) VALUES (src.id, (src.id % 7)::INTEGER);
CALL lake.set_option('rewrite_delete_threshold', 0);
CALL ducklake_rewrite_data_files('lake');
MERGE INTO t2 AS tgt USING (SELECT i AS id FROM range(1625, 3570) r(i)) AS src ON tgt.id = src.id WHEN MATCHED THEN UPDATE SET "c2" = CASE WHEN hash(src.id, 1208, 29) % 10 = 0 THEN NULL ELSE (CASE hash(src.id, 9127, 29) % 20 WHEN 0 THEN (0)::USMALLINT WHEN 1 THEN (65535)::USMALLINT ELSE ((hash(src.id, 9127, 29) % 65536)::HUGEINT - 0)::USMALLINT END) END WHEN NOT MATCHED THEN INSERT (id, grp) VALUES (src.id, (src.id % 7)::INTEGER);
