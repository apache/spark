
create table apachelog(ipaddress STRING,identd STRING,user_name STRING,finishtime STRING,requestline string,returncode INT,size INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe' WITH SERDEPROPERTIES (  'serialization.format'= 'org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol',  'quote.delim'= '("|\\[|\\])',  'field.delim'=' ',  'serialization.null.format'='-'  ) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../data/files/apache.access.log' INTO TABLE apachelog;
SELECT a.* FROM apachelog a;

