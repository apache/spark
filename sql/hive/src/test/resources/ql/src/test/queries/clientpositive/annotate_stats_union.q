set hive.stats.fetch.column.stats=true;

create table if not exists loc_staging (
  state string,
  locid int,
  zip bigint,
  year int
) row format delimited fields terminated by '|' stored as textfile;

create table loc_orc like loc_staging;
alter table loc_orc set fileformat orc;

load data local inpath '../../data/files/loc.txt' overwrite into table loc_staging;

insert overwrite table loc_orc select * from loc_staging;

analyze table loc_orc compute statistics for columns state,locid,zip,year;

-- numRows: 8 rawDataSize: 688
explain extended select state from loc_orc;

-- numRows: 16 rawDataSize: 1376
explain extended select * from (select state from loc_orc union all select state from loc_orc) tmp;

-- numRows: 8 rawDataSize: 796
explain extended select * from loc_orc;

-- numRows: 16 rawDataSize: 1592
explain extended select * from (select * from loc_orc union all select * from loc_orc) tmp;

create database test;
use test;
create table if not exists loc_staging (
  state string,
  locid int,
  zip bigint,
  year int
) row format delimited fields terminated by '|' stored as textfile;

create table loc_orc like loc_staging;
alter table loc_orc set fileformat orc;

load data local inpath '../../data/files/loc.txt' overwrite into table loc_staging;

insert overwrite table loc_orc select * from loc_staging;

analyze table loc_staging compute statistics;
analyze table loc_staging compute statistics for columns state,locid,zip,year;
analyze table loc_orc compute statistics for columns state,locid,zip,year;

-- numRows: 16 rawDataSize: 1376
explain extended select * from (select state from default.loc_orc union all select state from test.loc_orc) temp;

-- numRows: 16 rawDataSize: 1376
explain extended select * from (select state from test.loc_staging union all select state from test.loc_orc) temp;
