-- test table
create table test_table (id int, query string, name string);
describe extended test_table;

alter table test_table set serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';
describe extended test_table;

alter table test_table set serdeproperties ('field.delim' = ',');
describe extended test_table;

drop table test_table;

--- test partitioned table
create table test_table (id int, query string, name string) partitioned by (dt string);

alter table test_table add partition (dt = '2011');
describe extended test_table partition (dt='2011');

alter table test_table set serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';
describe extended test_table partition (dt='2011');

alter table test_table set serdeproperties ('field.delim' = ',');
describe extended test_table partition (dt='2011');

-- test partitions

alter table test_table partition(dt='2011') set serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';
describe extended test_table partition (dt='2011');

alter table test_table partition(dt='2011') set serdeproperties ('field.delim' = ',');
describe extended test_table partition (dt='2011');

drop table test_table
