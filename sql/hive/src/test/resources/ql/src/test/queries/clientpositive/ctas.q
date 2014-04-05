-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.20, 0.20S)

create table nzhang_Tmp(a int, b string);
select * from nzhang_Tmp;

explain create table nzhang_CTAS1 as select key k, value from src sort by k, value limit 10;

create table nzhang_CTAS1 as select key k, value from src sort by k, value limit 10;

select * from nzhang_CTAS1;

describe formatted nzhang_CTAS1;


explain create table nzhang_ctas2 as select * from src sort by key, value limit 10;

create table nzhang_ctas2 as select * from src sort by key, value limit 10;

select * from nzhang_ctas2;

describe formatted nzhang_CTAS2;


explain create table nzhang_ctas3 row format serde "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe" stored as RCFile as select key/2 half_key, concat(value, "_con") conb  from src sort by half_key, conb limit 10;

create table nzhang_ctas3 row format serde "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe" stored as RCFile as select key/2 half_key, concat(value, "_con") conb  from src sort by half_key, conb limit 10;

select * from nzhang_ctas3;

describe formatted nzhang_CTAS3;


explain create table if not exists nzhang_ctas3 as select key, value from src sort by key, value limit 2;

create table if not exists nzhang_ctas3 as select key, value from src sort by key, value limit 2;

select * from nzhang_ctas3;

describe formatted nzhang_CTAS3;


explain create table nzhang_ctas4 row format delimited fields terminated by ',' stored as textfile as select key, value from src sort by key, value limit 10;

create table nzhang_ctas4 row format delimited fields terminated by ',' stored as textfile as select key, value from src sort by key, value limit 10;

select * from nzhang_ctas4;

describe formatted nzhang_CTAS4;

explain extended create table nzhang_ctas5 row format delimited fields terminated by ',' lines terminated by '\012' stored as textfile as select key, value from src sort by key, value limit 10;

set mapreduce.framework.name=yarn;
set mapreduce.jobtracker.address=localhost:58;
set hive.exec.mode.local.auto=true;

create table nzhang_ctas5 row format delimited fields terminated by ',' lines terminated by '\012' stored as textfile as select key, value from src sort by key, value limit 10;

create table nzhang_ctas6 (key string, `to` string);
insert overwrite table nzhang_ctas6 select key, value from src limit 10;
create table nzhang_ctas7 as select key, `to` from nzhang_ctas6;









