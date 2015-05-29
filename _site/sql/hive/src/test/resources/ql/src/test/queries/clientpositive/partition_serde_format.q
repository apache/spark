create table src_part_serde (key int, value string) partitioned by (ds string) stored as sequencefile;
insert overwrite table src_part_serde partition (ds='2011') select * from src;
alter table src_part_serde set serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' with SERDEPROPERTIES ('serialization.format'='\t');
select key, value from src_part_serde where ds='2011' order by key, value limit 20;
