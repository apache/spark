dfs ${system:test.dfs.mkdir} file:///tmp/test;
dfs ${system:test.dfs.mkdir} hdfs:///tmp/test;

create external table dynPart (key string) partitioned by (value string) row format delimited fields terminated by '\\t' stored as textfile;
insert overwrite local directory "/tmp/test" select key from src where (key = 10) order by key;
insert overwrite directory "/tmp/test" select key from src where (key = 20) order by key;
alter table dynPart add partition (value='0') location 'file:///tmp/test';
alter table dynPart add partition (value='1') location 'hdfs:///tmp/test';
select count(*) from dynPart;
select key from dynPart;
select key from src where (key = 10) order by key;
select key from src where (key = 20) order by key;

dfs -rmr file:///tmp/test;
dfs -rmr hdfs:///tmp/test;
