CREATE TABLE citl_table (key STRING, value STRING) PARTITIONED BY (part STRING)
STORED AS RCFILE
LOCATION 'pfile:${system:test.tmp.dir}/citl_table';

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyPartitionIsSubdirectoryOfTableHook;

INSERT OVERWRITE TABLE citl_table PARTITION (part = '1') SELECT * FROM src;

SET hive.exec.post.hooks=;

ALTER TABLE citl_table SET LOCATION 'file:${system:test.tmp.dir}/citl_table';

ALTER TABLE citl_table PARTITION (part = '1') CONCATENATE;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyPartitionIsSubdirectoryOfTableHook;

SELECT count(*) FROM citl_table where part = '1';

SET hive.exec.post.hooks=;

DROP TABLE citl_table;
