
CREATE TABLE inv_valid_tbl1 COMMENT 'This is a thrift based table'
    PARTITIONED BY(aint DATETIME, country STRING)
    CLUSTERED BY(aint) SORTED BY(lint) INTO 32 BUCKETS
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
    WITH SERDEPROPERTIES ('serialization.class' = 'org.apache.hadoop.hive.serde2.thrift.test.Complex',
                          'serialization.format' = 'org.apache.thrift.protocol.TBinaryProtocol')
    STORED AS SEQUENCEFILE;
DESCRIBE EXTENDED inv_valid_tbl1;
