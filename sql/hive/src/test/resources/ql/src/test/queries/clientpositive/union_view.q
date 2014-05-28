CREATE TABLE src_union_1 (key int, value string) PARTITIONED BY (ds string);
CREATE INDEX src_union_1_key_idx ON TABLE src_union_1(key) AS 'COMPACT' WITH DEFERRED REBUILD;

CREATE TABLE src_union_2 (key int, value string) PARTITIONED BY (ds string, part_1 string);
CREATE INDEX src_union_2_key_idx ON TABLE src_union_2(key) AS 'COMPACT' WITH DEFERRED REBUILD;

CREATE TABLE src_union_3(key int, value string) PARTITIONED BY (ds string, part_1 string, part_2 string);
CREATE INDEX src_union_3_key_idx ON TABLE src_union_3(key) AS 'COMPACT' WITH DEFERRED REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

SET hive.exec.pre.hooks=;
SET hive.exec.post.hooks=;
SET hive.semantic.analyzer.hook=;
SET hive.merge.mapfiles=false;
SET hive.merge.mapredfiles=false;

INSERT OVERWRITE TABLE src_union_1 PARTITION (ds='1') SELECT * FROM src;
ALTER INDEX src_union_1_key_idx ON src_union_1 PARTITION (ds='1') REBUILD;

INSERT OVERWRITE TABLE src_union_2 PARTITION (ds='2', part_1='1') SELECT * FROM src;
INSERT OVERWRITE TABLE src_union_2 PARTITION (ds='2', part_1='2') SELECT * FROM src;
ALTER INDEX src_union_2_key_idx ON src_union_2 PARTITION (ds='2', part_1='1') REBUILD;
ALTER INDEX src_union_2_key_idx ON src_union_2 PARTITION (ds='2', part_1='2') REBUILD;

INSERT OVERWRITE TABLE src_union_3 PARTITION (ds='3', part_1='1', part_2='2:3+4') SELECT * FROM src;
INSERT OVERWRITE TABLE src_union_3 PARTITION (ds='3', part_1='2', part_2='2:3+4') SELECT * FROM src;
ALTER INDEX src_union_3_key_idx ON src_union_3 PARTITION (ds='3', part_1='1', part_2='2:3+4') REBUILD;
ALTER INDEX src_union_3_key_idx ON src_union_3 PARTITION (ds='3', part_1='2', part_2='2:3+4') REBUILD;

EXPLAIN SELECT key, value, ds FROM src_union_1 WHERE key=86 and ds='1';
EXPLAIN SELECT key, value, ds FROM src_union_2 WHERE key=86 and ds='2';
EXPLAIN SELECT key, value, ds FROM src_union_3 WHERE key=86 and ds='3';

SELECT key, value, ds FROM src_union_1 WHERE key=86 AND ds ='1';
SELECT key, value, ds FROM src_union_2 WHERE key=86 AND ds ='2';
SELECT key, value, ds FROM src_union_3 WHERE key=86 AND ds ='3';

EXPLAIN SELECT count(1) from src_union_1 WHERE ds ='1';
EXPLAIN SELECT count(1) from src_union_2 WHERE ds ='2';
EXPLAIN SELECT count(1) from src_union_3 WHERE ds ='3';

SELECT count(1) from src_union_1 WHERE ds ='1';
SELECT count(1) from src_union_2 WHERE ds ='2';
SELECT count(1) from src_union_3 WHERE ds ='3';

CREATE VIEW src_union_view PARTITIONED ON (ds) as
SELECT key, value, ds FROM (
SELECT key, value, ds FROM src_union_1
UNION ALL
SELECT key, value, ds FROM src_union_2
UNION ALL
SELECT key, value, ds FROM src_union_3
) subq;

EXPLAIN SELECT key, value, ds FROM src_union_view WHERE key=86 AND ds ='1';
EXPLAIN SELECT key, value, ds FROM src_union_view WHERE key=86 AND ds ='2';
EXPLAIN SELECT key, value, ds FROM src_union_view WHERE key=86 AND ds ='3';
EXPLAIN SELECT key, value, ds FROM src_union_view WHERE key=86 AND ds IS NOT NULL order by ds;

SELECT key, value, ds FROM src_union_view WHERE key=86 AND ds ='1';
SELECT key, value, ds FROM src_union_view WHERE key=86 AND ds ='2';
SELECT key, value, ds FROM src_union_view WHERE key=86 AND ds ='3';
SELECT key, value, ds FROM src_union_view WHERE key=86 AND ds IS NOT NULL order by ds;

EXPLAIN SELECT count(1) from src_union_view WHERE ds ='1';
EXPLAIN SELECT count(1) from src_union_view WHERE ds ='2';
EXPLAIN SELECT count(1) from src_union_view WHERE ds ='3';

SELECT count(1) from src_union_view WHERE ds ='1';
SELECT count(1) from src_union_view WHERE ds ='2';
SELECT count(1) from src_union_view WHERE ds ='3';

INSERT OVERWRITE TABLE src_union_3 PARTITION (ds='4', part_1='1', part_2='2:3+4') SELECT * FROM src;
ALTER INDEX src_union_3_key_idx ON src_union_3 PARTITION (ds='4', part_1='1', part_2='2:3+4') REBUILD;

EXPLAIN SELECT key, value, ds FROM src_union_view WHERE key=86 AND ds ='4';
SELECT key, value, ds FROM src_union_view WHERE key=86 AND ds ='4';

EXPLAIN SELECT count(1) from src_union_view WHERE ds ='4';
SELECT count(1) from src_union_view WHERE ds ='4';
