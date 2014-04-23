-- This test verifies that if the tables location changes, renaming a table will not change
-- the table location scheme

CREATE TABLE rename_partition_table (key STRING, value STRING) PARTITIONED BY (part STRING)
STORED AS RCFILE
LOCATION 'pfile:${system:test.tmp.dir}/rename_partition_table';

INSERT OVERWRITE TABLE rename_partition_table PARTITION (part = '1') SELECT * FROM src;

ALTER TABLE rename_partition_table SET LOCATION 'file:${system:test.tmp.dir}/rename_partition_table';

set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyOutputTableLocationSchemeIsFileHook;

-- If the metastore attempts to change the scheme of the table back to the default pfile, it will get
-- an exception related to the source and destination file systems not matching

ALTER TABLE rename_partition_table RENAME TO rename_partition_table_renamed;
