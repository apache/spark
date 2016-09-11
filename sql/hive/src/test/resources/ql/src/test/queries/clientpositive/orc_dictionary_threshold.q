set hive.exec.orc.dictionary.key.size.threshold=-1;

-- Set the threshold to -1 to guarantee dictionary encoding is turned off
-- Tests that the data can be read back correctly when a string column is stored
-- without dictionary encoding

CREATE TABLE test_orc (key STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

-- should be single split
INSERT OVERWRITE TABLE test_orc SELECT key FROM src TABLESAMPLE (10 ROWS);

-- Test reading the column back

SELECT * FROM test_orc; 

ALTER TABLE test_orc SET SERDEPROPERTIES ('orc.stripe.size' = '1');

CREATE TABLE src_thousand(key STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv1kv2.cogroup.txt' 
     INTO TABLE src_thousand;

set hive.exec.orc.dictionary.key.size.threshold=0.5;

-- Add data to the table in such a way that alternate stripes encode the column
-- differently.  Setting orc.stripe.size = 1 guarantees the stripes each have 
-- 5000 rows.  The first stripe will have 5 * 630 distinct rows and thus be
-- above the cutoff of 50% and will be direct encoded. The second stripe
-- will have 5 * 1 distinct rows and thus be under the cutoff and will be
-- dictionary encoded. The final stripe will have 630 out of 1000 and be 
-- direct encoded.

INSERT OVERWRITE TABLE test_orc
SELECT key FROM (
SELECT CONCAT("a", key) AS key FROM src_thousand
UNION ALL
SELECT CONCAT("b", key) AS key FROM src_thousand
UNION ALL
SELECT CONCAT("c", key) AS key FROM src_thousand
UNION ALL
SELECT CONCAT("d", key) AS key FROM src_thousand
UNION ALL
SELECT CONCAT("e", key) AS key FROM src_thousand
UNION ALL
SELECT CONCAT("f", 1) AS key FROM src_thousand
UNION ALL
SELECT CONCAT("g", 1) AS key FROM src_thousand
UNION ALL
SELECT CONCAT("h", 1) AS key FROM src_thousand
UNION ALL
SELECT CONCAT("i", 1) AS key FROM src_thousand
UNION ALL
SELECT CONCAT("j", 1) AS key FROM src_thousand
UNION ALL
SELECT CONCAT("k", key) AS key FROM src_thousand
) a ORDER BY key LIMIT 11000;

SELECT SUM(HASH(key)) FROM test_orc;
