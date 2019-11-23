dfs ${system:test.dfs.mkdir} hdfs:///tmp/udf_using;

dfs -copyFromLocal ../../data/files/sales.txt hdfs:///tmp/udf_using/sales.txt;

create function lookup as 'org.apache.hadoop.hive.ql.udf.UDFFileLookup' using file 'hdfs:///tmp/udf_using/sales.txt';

create table udf_using (c1 string);
insert overwrite table udf_using select 'Joe' from src limit 2;

select c1, lookup(c1) from udf_using;

drop table udf_using;
drop function lookup;

dfs -rmr hdfs:///tmp/udf_using;
