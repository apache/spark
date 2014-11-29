set datanucleus.cache.collections=false;
set hive.stats.autogather=true;
drop table hive_test_src;
drop table hive_test_dst;

create table hive_test_src ( col1 string ) stored as textfile ;
explain extended
load data local inpath '../../data/files/test.dat' overwrite into table hive_test_src ;

load data local inpath '../../data/files/test.dat' overwrite into table hive_test_src ;

desc formatted hive_test_src;

create table hive_test_dst ( col1 string ) partitioned by ( pcol1 string , pcol2 string) stored as sequencefile;
insert overwrite table hive_test_dst partition ( pcol1='test_part', pCol2='test_Part') select col1 from hive_test_src ;
select * from hive_test_dst where pcol1='test_part' and pcol2='test_Part';

select count(1) from hive_test_dst;

insert overwrite table hive_test_dst partition ( pCol1='test_part', pcol2='test_Part') select col1 from hive_test_src ;
select * from hive_test_dst where pcol1='test_part' and pcol2='test_part';

select count(1) from hive_test_dst;

select * from hive_test_dst where pcol1='test_part';
select * from hive_test_dst where pcol1='test_part' and pcol2='test_part';
select * from hive_test_dst where pcol1='test_Part';

describe formatted hive_test_dst;

drop table hive_test_src;
drop table hive_test_dst;
