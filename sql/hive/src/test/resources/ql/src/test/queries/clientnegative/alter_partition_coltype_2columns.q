-- create testing table
create table alter_coltype(key string, value string) partitioned by (dt string, ts string);

-- insert and create a partition
insert overwrite table alter_coltype partition(dt='100x', ts='6:30pm') select * from src1;

desc alter_coltype;

-- alter partition change multiple keys at same time 
alter table alter_coltype partition column (dt int, ts int);

