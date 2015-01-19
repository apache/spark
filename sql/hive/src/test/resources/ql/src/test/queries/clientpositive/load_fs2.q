-- HIVE-3300 [jira] LOAD DATA INPATH fails if a hdfs file with same name is added to table
-- 'loader' table is used only for uploading kv1.txt to HDFS (!hdfs -put is not working on minMRDriver)

create table result (key string, value string);
create table loader (key string, value string);

load data local inpath '../../data/files/kv1.txt' into table loader;

load data inpath '/build/ql/test/data/warehouse/loader/kv1.txt' into table result;
show table extended like result;

load data local inpath '../../data/files/kv1.txt' into table loader;

load data inpath '/build/ql/test/data/warehouse/loader/kv1.txt' into table result;
show table extended like result;

load data local inpath '../../data/files/kv1.txt' into table loader;

load data inpath '/build/ql/test/data/warehouse/loader/kv1.txt' into table result;
show table extended like result;
