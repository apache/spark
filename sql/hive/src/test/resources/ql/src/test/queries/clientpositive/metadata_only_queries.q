set hive.stats.dbclass=fs;
set hive.compute.query.using.stats=true;
set hive.stats.autogather=true;
create table over10k(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp, 
           dec decimal,  
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over10k' into table over10k;

create table stats_tbl(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
	   ts timestamp, 
           dec decimal,  
           bin binary);

create table stats_tbl_part(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
	   ts timestamp, 
           dec decimal,  
           bin binary) partitioned by (dt string);


insert overwrite table stats_tbl select * from over10k;

insert into table stats_tbl_part partition (dt='2010') select * from over10k where t>0 and t<30;
insert into table stats_tbl_part partition (dt='2011') select * from over10k where t>30 and t<60;
insert into table stats_tbl_part partition (dt='2012') select * from over10k where t>60;

explain 
select count(*), sum(1), sum(0.2), count(1), count(s), count(bo), count(bin), count(si), max(i), min(b) from stats_tbl;
explain
select count(*), sum(1), sum(0.2), count(1), count(s), count(bo), count(bin), count(si), max(i), min(b) from stats_tbl_part;

analyze table stats_tbl compute statistics for columns t,si,i,b,f,d,bo,s,bin;
analyze table stats_tbl_part partition(dt='2010') compute statistics for columns t,si,i,b,f,d,bo,s,bin;
analyze table stats_tbl_part partition(dt='2011') compute statistics for columns t,si,i,b,f,d,bo,s,bin;
analyze table stats_tbl_part partition(dt='2012') compute statistics for columns t,si,i,b,f,d,bo,s,bin;

explain 
select count(*), sum(1), sum(0.2), count(1), count(s), count(bo), count(bin), count(si), max(i), min(b), max(f), min(d) from stats_tbl;
select count(*), sum(1), sum(0.2), count(1), count(s), count(bo), count(bin), count(si), max(i), min(b), max(f), min(d) from stats_tbl;
explain 
select count(*), sum(1), sum(0.2), count(1), count(s), count(bo), count(bin), count(si), max(i), min(b), max(f), min(d) from stats_tbl_part;
select count(*), sum(1), sum(0.2), count(1), count(s), count(bo), count(bin), count(si), max(i), min(b), max(f), min(d) from stats_tbl_part;

explain select count(ts) from stats_tbl_part;

drop table stats_tbl;
drop table stats_tbl_part;

set hive.compute.query.using.stats=false;
set hive.stats.dbclass=jdbc:derby;
