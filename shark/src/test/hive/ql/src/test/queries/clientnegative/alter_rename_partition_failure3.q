create table alter_rename_partition_src ( col1 string ) stored as textfile ;
load data local inpath '../data/files/test.dat' overwrite into table alter_rename_partition_src ;
create table alter_rename_partition ( col1 string ) partitioned by (pcol1 string , pcol2 string) stored as sequencefile;
insert overwrite table alter_rename_partition partition (pCol1='old_part1:', pcol2='old_part2:') select col1 from alter_rename_partition_src ;

alter table alter_rename_partition partition (pCol1='old_part1:', pcol2='old_part2:') rename to partition (pCol1='old_part1:', pcol2='old_part2:', pcol3='old_part3:');