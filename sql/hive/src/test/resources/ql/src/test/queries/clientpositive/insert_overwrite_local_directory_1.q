insert overwrite local directory '../../data/files/local_src_table_1'
select * from src ;
dfs -cat ../../data/files/local_src_table_1/000000_0;

insert overwrite local directory '../../data/files/local_src_table_2'
row format delimited 
FIELDS TERMINATED BY ':' 
select * from src ;

dfs -cat ../../data/files/local_src_table_2/000000_0;

create table array_table (a array<string>, b array<string>)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ',';

load data local inpath "../../data/files/array_table.txt" overwrite into table array_table;

insert overwrite local directory '../../data/files/local_array_table_1'
select * from array_table;
dfs -cat ../../data/files/local_array_table_1/000000_0;

insert overwrite local directory '../../data/files/local_array_table_2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ':'
COLLECTION ITEMS TERMINATED BY '#'
select * from array_table;

dfs -cat ../../data/files/local_array_table_2/000000_0;

insert overwrite local directory '../../data/files/local_array_table_2_withfields'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ':'
COLLECTION ITEMS TERMINATED BY '#'
select b,a from array_table;

dfs -cat ../../data/files/local_array_table_2_withfields/000000_0;


create table map_table (foo STRING , bar MAP<STRING, STRING>)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;

load data local inpath "../../data/files/map_table.txt" overwrite into table map_table;

insert overwrite local directory '../../data/files/local_map_table_1'
select * from map_table;
dfs -cat ../../data/files/local_map_table_1/000000_0;

insert overwrite local directory '../../data/files/local_map_table_2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ':'
COLLECTION ITEMS TERMINATED BY '#'
MAP KEYS TERMINATED BY '='
select * from map_table;

dfs -cat ../../data/files/local_map_table_2/000000_0;

insert overwrite local directory '../../data/files/local_map_table_2_withfields'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ':'
COLLECTION ITEMS TERMINATED BY '#'
MAP KEYS TERMINATED BY '='
select bar,foo from map_table;

dfs -cat ../../data/files/local_map_table_2_withfields/000000_0;

insert overwrite local directory '../../data/files/local_array_table_3'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.DelimitedJSONSerDe'
STORED AS TEXTFILE
select * from array_table;

dfs -cat ../../data/files/local_array_table_3/000000_0;

insert overwrite local directory '../../data/files/local_map_table_3'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.DelimitedJSONSerDe'
STORED AS TEXTFILE
select * from map_table;

dfs -cat ../../data/files/local_map_table_3/000000_0;

insert overwrite local directory '../../data/files/local_rctable'
STORED AS RCFILE
select value,key from src;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/local_rctable/temp;
dfs -rmr ${system:test.tmp.dir}/local_rctable;
dfs ${system:test.dfs.mkdir}  ${system:test.tmp.dir}/local_rctable;
dfs -put ../../data/files/local_rctable/000000_0 ${system:test.tmp.dir}/local_rctable/000000_0;

create external table local_rctable(value string, key string)
STORED AS RCFILE
LOCATION '${system:test.tmp.dir}/local_rctable';

insert overwrite local directory '../../data/files/local_rctable_out'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
select key,value from local_rctable;

dfs -cat ../../data/files/local_rctable_out/000000_0;

drop table local_rctable;
drop table array_table;
drop table map_table;
dfs -rmr ${system:test.tmp.dir}/local_rctable;

