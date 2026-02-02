-- This test verifies that if the tables location changes, renaming a partition will not change
-- the partition location accordingly

CREATE TABLE rename_partition_table (key STRING, value STRING) PARTITIONED BY (part STRING)
STORED AS RCFILE
LOCATION 'pfile:${system:test.tmp.dir}/rename_partition_table';

INSERT OVERWRITE TABLE rename_partition_table PARTITION (part = '1') SELECT * FROM src;

ALTER TABLE rename_partition_table SET LOCATION 'file:${system:test.tmp.dir}/rename_partition_table';

ALTER TABLE rename_partition_table PARTITION (part = '1') RENAME TO PARTITION (part = '2');

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyPartitionIsNotSubdirectoryOfTableHook;

SELECT count(*) FROM rename_partition_table where part = '2';

SET hive.exec.post.hooks=;

DROP TABLE rename_partition_table;
