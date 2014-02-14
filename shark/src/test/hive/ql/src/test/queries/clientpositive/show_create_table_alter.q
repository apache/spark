-- Test SHOW CREATE TABLE on an external, clustered and sorted table. Then test the query again after ALTERs.

CREATE EXTERNAL TABLE tmp_showcrt1 (key smallint, value float)
CLUSTERED BY (key) SORTED BY (value DESC) INTO 5 BUCKETS;
SHOW CREATE TABLE tmp_showcrt1;

-- Add a comment to the table, change the EXTERNAL property, and test SHOW CREATE TABLE on the change.
ALTER TABLE tmp_showcrt1 SET TBLPROPERTIES ('comment'='temporary table', 'EXTERNAL'='FALSE');
SHOW CREATE TABLE tmp_showcrt1;

-- Alter the table comment, change the EXTERNAL property back and test SHOW CREATE TABLE on the change.
ALTER TABLE tmp_showcrt1 SET TBLPROPERTIES ('comment'='changed comment', 'EXTERNAL'='TRUE');
SHOW CREATE TABLE tmp_showcrt1;

-- Change the 'SORTBUCKETCOLSPREFIX' property and test SHOW CREATE TABLE. The output should not change.
ALTER TABLE tmp_showcrt1 SET TBLPROPERTIES ('SORTBUCKETCOLSPREFIX'='FALSE');
SHOW CREATE TABLE tmp_showcrt1;

-- Alter the storage handler of the table, and test SHOW CREATE TABLE.
ALTER TABLE tmp_showcrt1 SET TBLPROPERTIES ('storage_handler'='org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler');
SHOW CREATE TABLE tmp_showcrt1;
DROP TABLE tmp_showcrt1;

