-- Create table
create table if not exists alter_part_nodrop_table(key string, value string ) partitioned by (year string, month string) stored as textfile ;

-- Load data
load data local inpath '../../data/files/T1.txt' overwrite into table alter_part_nodrop_table partition (year='1996', month='10');
load data local inpath '../../data/files/T1.txt' overwrite into table alter_part_nodrop_table partition (year='1996', month='12');

alter table alter_part_nodrop_table partition (year='1996') enable no_drop;
drop table alter_part_nodrop_table;
