drop table partition_date2_1;

create table partition_date2_1 (key string, value string) partitioned by (dt date, region int);

-- test date literal syntax
from (select * from src tablesample (1 rows)) x
insert overwrite table partition_date2_1 partition(dt=date '2000-01-01', region=1) select *
insert overwrite table partition_date2_1 partition(dt=date '2000-01-01', region=2) select *
insert overwrite table partition_date2_1 partition(dt=date '1999-01-01', region=2) select *;

select distinct dt from partition_date2_1;
select * from partition_date2_1;

-- insert overwrite
insert overwrite table partition_date2_1 partition(dt=date '2000-01-01', region=2) 
  select 'changed_key', 'changed_value' from src tablesample (2 rows);
select * from partition_date2_1;

-- truncate
truncate table partition_date2_1 partition(dt=date '2000-01-01', region=2);
select distinct dt from partition_date2_1;
select * from partition_date2_1;

-- alter table add partition
alter table partition_date2_1 add partition (dt=date '1980-01-02', region=3);
select distinct dt from partition_date2_1;
select * from partition_date2_1;

-- alter table drop
alter table partition_date2_1 drop partition (dt=date '1999-01-01', region=2);
select distinct dt from partition_date2_1;
select * from partition_date2_1;

-- alter table set serde
alter table partition_date2_1 partition(dt=date '1980-01-02', region=3) 
  set serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';

-- alter table set fileformat
alter table partition_date2_1 partition(dt=date '1980-01-02', region=3)
  set fileformat rcfile;
describe extended partition_date2_1  partition(dt=date '1980-01-02', region=3);

insert overwrite table partition_date2_1 partition(dt=date '1980-01-02', region=3)
  select * from src tablesample (2 rows);
select * from partition_date2_1 order by key,value,dt,region;

-- alter table set location
alter table partition_date2_1 partition(dt=date '1980-01-02', region=3)
  set location "file:///tmp/partition_date2_1";
describe extended partition_date2_1 partition(dt=date '1980-01-02', region=3);

-- alter table touch
alter table partition_date2_1 touch partition(dt=date '1980-01-02', region=3);

drop table partition_date2_1;
