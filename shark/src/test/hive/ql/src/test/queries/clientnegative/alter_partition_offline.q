-- create table
create table if not exists alter_part_offline (key string, value string ) partitioned by (year string, month string) stored as textfile ;

-- Load data
load data local inpath '../data/files/T1.txt' overwrite into table alter_part_offline partition (year='1996', month='10');
load data local inpath '../data/files/T1.txt' overwrite into table alter_part_offline partition (year='1996', month='12');

alter table alter_part_offline partition (year='1996') disable offline;
select * from alter_part_offline where year = '1996';
alter table alter_part_offline partition (year='1996') enable offline;
select * from alter_part_offline where year = '1996';
