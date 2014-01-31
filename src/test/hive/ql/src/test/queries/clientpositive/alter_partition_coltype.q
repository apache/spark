-- create testing table.
create table alter_coltype(key string, value string) partitioned by (dt string, ts string);

-- insert and create a partition.
insert overwrite table alter_coltype partition(dt='100x', ts='6:30pm') select * from src1;

desc alter_coltype;

-- select with paritition predicate.
select count(*) from alter_coltype where dt = '100x';

-- alter partition key column data type for dt column.
alter table alter_coltype partition column (dt int);

-- load a new partition using new data type.
insert overwrite table alter_coltype partition(dt=10, ts='3.0') select * from src1;

-- make sure the partition predicate still works. 
select count(*) from alter_coltype where dt = '100x';
explain extended select count(*) from alter_coltype where dt = '100x';

select count(*) from alter_coltype where dt = 100;

-- alter partition key column data type for ts column.
alter table alter_coltype partition column (ts double);

alter table alter_coltype partition column (dt string);

-- load a new partition using new data type.
insert overwrite table alter_coltype partition(dt='100x', ts=3.0) select * from src1;

--  validate partition key column predicate can still work.
select count(*) from alter_coltype where ts = '6:30pm';
explain extended select count(*) from alter_coltype where ts = '6:30pm';

--  validate partition key column predicate on two different partition column data type 
--  can still work.
select count(*) from alter_coltype where ts = 3.0 and dt=10;
explain extended select count(*) from alter_coltype where ts = 3.0 and dt=10;

-- query where multiple partition values (of different datatypes) are being selected 
select key, value, dt, ts from alter_coltype where dt is not null;
explain extended select key, value, dt, ts from alter_coltype where dt is not null;

select count(*) from alter_coltype where ts = 3.0;

-- make sure the partition predicate still works. 
select count(*) from alter_coltype where dt = '100x' or dt = '10';
explain extended select count(*) from alter_coltype where dt = '100x' or dt = '10';

desc alter_coltype;
desc alter_coltype partition (dt='100x', ts='6:30pm');
desc alter_coltype partition (dt='100x', ts=3.0);
desc alter_coltype partition (dt=10, ts=3.0);

drop table alter_coltype;

