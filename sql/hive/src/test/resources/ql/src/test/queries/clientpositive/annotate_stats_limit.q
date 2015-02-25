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

analyze table loc_orc compute statistics for columns state, locid, zip, year;

-- numRows: 8 rawDataSize: 796
explain extended select * from loc_orc;

-- numRows: 4 rawDataSize: 396
explain extended select * from loc_orc limit 4;

-- greater than the available number of rows
-- numRows: 8 rawDataSize: 796
explain extended select * from loc_orc limit 16;

-- numRows: 0 rawDataSize: 0
explain extended select * from loc_orc limit 0;
