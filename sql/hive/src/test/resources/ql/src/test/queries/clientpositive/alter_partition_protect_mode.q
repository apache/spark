-- Create table
create table if not exists alter_part_protect_mode(key string, value string ) partitioned by (year string, month string) stored as textfile ;

-- Load data
load data local inpath '../../data/files/T1.txt' overwrite into table alter_part_protect_mode partition (year='1996', month='10');
load data local inpath '../../data/files/T1.txt' overwrite into table alter_part_protect_mode partition (year='1996', month='12');
load data local inpath '../../data/files/T1.txt' overwrite into table alter_part_protect_mode partition (year='1995', month='09');
load data local inpath '../../data/files/T1.txt' overwrite into table alter_part_protect_mode partition (year='1994', month='07');

-- offline
alter table alter_part_protect_mode partition (year='1996') disable offline;
select * from alter_part_protect_mode where year = '1996';
alter table alter_part_protect_mode partition (year='1995') enable offline;
alter table alter_part_protect_mode partition (year='1995') disable offline;
select * from alter_part_protect_mode where year = '1995';

-- no_drop
alter table alter_part_protect_mode partition (year='1996') enable no_drop;
alter table alter_part_protect_mode partition (year='1995') disable no_drop;
alter table alter_part_protect_mode drop partition (year='1995');
alter table alter_part_protect_mode partition (year='1994', month='07') disable no_drop;
alter table alter_part_protect_mode drop partition (year='1994');

-- Cleanup
alter table alter_part_protect_mode partition (year='1996') disable no_drop;
drop table alter_part_protect_mode;
