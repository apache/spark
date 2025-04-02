set hive.stats.dbclass=fs;
drop index src_index_2 on src;
drop index src_index_3 on src;
drop index src_index_4 on src;
drop index src_index_5 on src;
drop index src_index_6 on src;
drop index src_index_7 on src;
drop index src_index_8 on src;
drop index src_index_9 on src;
drop table `_t`;

create index src_index_2 on table src(key) as 'compact' WITH DEFERRED REBUILD;
desc extended default__src_src_index_2__;

create index src_index_3 on table src(key) as 'compact' WITH DEFERRED REBUILD in table src_idx_src_index_3;
desc extended src_idx_src_index_3;

create index src_index_4 on table src(key) as 'compact' WITH DEFERRED REBUILD ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
desc extended default__src_src_index_4__;

create index src_index_5 on table src(key) as 'compact' WITH DEFERRED REBUILD ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ESCAPED BY '\\';
desc extended default__src_src_index_5__;

create index src_index_6 on table src(key) as 'compact' WITH DEFERRED REBUILD STORED AS RCFILE;
desc extended default__src_src_index_6__;

create index src_index_7 on table src(key) as 'compact' WITH DEFERRED REBUILD in table src_idx_src_index_7 STORED AS RCFILE; 
desc extended src_idx_src_index_7;

create index src_index_8 on table src(key) as 'compact' WITH DEFERRED REBUILD IDXPROPERTIES ("prop1"="val1", "prop2"="val2"); 
desc extended default__src_src_index_8__;

create index src_index_9 on table src(key) as 'compact' WITH DEFERRED REBUILD TBLPROPERTIES ("prop1"="val1", "prop2"="val2"); 
desc extended default__src_src_index_9__;

create table `_t`(`_i` int, `_j` int);
create index x on table `_t`(`_j`) as 'compact' WITH DEFERRED REBUILD;
alter index x on `_t` rebuild;

create index x2 on table `_t`(`_i`,`_j`) as 'compact' WITH DEFERRED
REBUILD;
alter index x2 on `_t` rebuild;

drop index src_index_2 on src;
drop index src_index_3 on src;
drop index src_index_4 on src;
drop index src_index_5 on src;
drop index src_index_6 on src;
drop index src_index_7 on src;
drop index src_index_8 on src;
drop index src_index_9 on src;
drop table `_t`;

show tables;
