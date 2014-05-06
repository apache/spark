
CREATE TABLE INPUTDDL8 COMMENT 'This is a thrift based table'
    PARTITIONED BY(ds STRING, country STRING)
    CLUSTERED BY(aint) SORTED BY(lint) INTO 32 BUCKETS
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
    WITH SERDEPROPERTIES ('serialization.class' = 'org.apache.hadoop.hive.serde2.thrift.test.Complex',
                          'serialization.format' = 'com.facebook.thrift.protocol.TBinaryProtocol')
    STORED AS SEQUENCEFILE;
DESCRIBE EXTENDED INPUTDDL8;

