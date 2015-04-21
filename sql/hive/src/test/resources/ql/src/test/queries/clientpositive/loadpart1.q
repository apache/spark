


create table hive_test_src ( col1 string ) stored as textfile ;
load data local inpath '../../data/files/test.dat' overwrite into table hive_test_src ;

create table hive_test_dst ( col1 string ) partitioned by ( pcol1 string , pcol2 string) stored as sequencefile;
insert overwrite table hive_test_dst partition ( pcol1='test_part', pCol2='test_Part') select col1 from hive_test_src ;
select * from hive_test_dst where pcol1='test_part' and pcol2='test_Part';

insert overwrite table hive_test_dst partition ( pCol1='test_part', pcol2='test_Part') select col1 from hive_test_src ;
select * from hive_test_dst where pcol1='test_part' and pcol2='test_part';

select * from hive_test_dst where pcol1='test_part';
select * from hive_test_dst where pcol1='test_part' and pcol2='test_part';
select * from hive_test_dst where pcol1='test_Part';



