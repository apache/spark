set hive.stats.dbclass=fs;
set hive.compute.query.using.stats=true;
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
           bin binary) partitioned by (dt int);


from over10k 
insert overwrite table stats_tbl_part partition (dt=2010) select t,si,i,b,f,d,bo,s,ts,dec,bin where t>0 and t<30 
insert overwrite table stats_tbl_part partition (dt=2014) select t,si,i,b,f,d,bo,s,ts,dec,bin where t > 30 and t<60;

analyze table stats_tbl_part partition(dt) compute statistics;
analyze table stats_tbl_part partition(dt=2010) compute statistics for columns t,si,i,b,f,d,bo,s,bin;
analyze table stats_tbl_part partition(dt=2014) compute statistics for columns t,si,i,b,f,d,bo,s,bin;

explain 
select count(*), count(1), sum(1), count(s), count(bo), count(bin), count(si), max(i), min(b), max(f), min(d) from stats_tbl_part where dt = 2010;
select count(*), count(1), sum(1), count(s), count(bo), count(bin), count(si), max(i), min(b), max(f), min(d) from stats_tbl_part where dt = 2010;
explain 
select count(*), count(1), sum(1), sum(2), count(s), count(bo), count(bin), count(si), max(i), min(b), max(f), min(d) from stats_tbl_part where dt > 2010;
select count(*), count(1), sum(1), sum(2), count(s), count(bo), count(bin), count(si), max(i), min(b), max(f), min(d) from stats_tbl_part where dt > 2010;

drop table stats_tbl_part;
set hive.compute.query.using.stats=false;
set hive.stats.dbclass=jdbc:derby;
