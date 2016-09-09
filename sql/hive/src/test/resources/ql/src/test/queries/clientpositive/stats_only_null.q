set hive.stats.dbclass=fs;
set hive.compute.query.using.stats=true;
set hive.stats.autogather=true;
CREATE TABLE temps_null(a double, b int, c STRING, d smallint) STORED AS TEXTFILE; 

CREATE TABLE stats_null(a double, b int, c STRING, d smallint) STORED AS TEXTFILE; 

CREATE TABLE stats_null_part(a double, b int, c STRING, d smallint) partitioned by (dt string) STORED AS TEXTFILE; 

LOAD DATA LOCAL INPATH '../../data/files/null.txt' INTO TABLE temps_null;

insert overwrite table stats_null select * from temps_null;
insert into table stats_null_part partition(dt='2010') select * from temps_null where d <=5;

insert into table stats_null_part partition(dt='2011') select * from temps_null where d > 5;
explain 
select count(*), count(a), count(b), count(c), count(d) from stats_null;
explain 
select count(*), count(a), count(b), count(c), count(d) from stats_null_part;


analyze table stats_null compute statistics for columns a,b,c,d;
analyze table stats_null_part partition(dt='2010') compute statistics for columns a,b,c,d;
analyze table stats_null_part partition(dt='2011') compute statistics for columns a,b,c,d;

describe formatted stats_null_part partition (dt='2010');
describe formatted stats_null_part partition (dt='2011');

explain 
select count(*), count(a), count(b), count(c), count(d) from stats_null;
explain 
select count(*), count(a), count(b), count(c), count(d) from stats_null_part;


select count(*), count(a), count(b), count(c), count(d) from stats_null;
select count(*), count(a), count(b), count(c), count(d) from stats_null_part;
drop table stats_null;
drop table stats_null_part;
drop table temps_null;
set hive.compute.query.using.stats=false;
set hive.stats.dbclass=jdbc:derby;
