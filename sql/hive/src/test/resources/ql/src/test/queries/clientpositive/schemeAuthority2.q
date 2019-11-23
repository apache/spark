dfs ${system:test.dfs.mkdir} file:///tmp/test;
dfs ${system:test.dfs.mkdir} hdfs:///tmp/test;

create external table dynPart (key string) partitioned by (value string, value2 string) row format delimited fields terminated by '\\t' stored as textfile;
insert overwrite local directory "/tmp/test" select key from src where (key = 10) order by key;
insert overwrite directory "/tmp/test" select key from src where (key = 20) order by key;
alter table dynPart add partition (value='0', value2='clusterA') location 'file:///tmp/test';
alter table dynPart add partition (value='0', value2='clusterB') location 'hdfs:///tmp/test';
select value2, key from dynPart where value='0';

dfs -rmr file:///tmp/test;
dfs -rmr hdfs:///tmp/test;
