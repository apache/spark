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

-- numRows: 8 rawDataSize: 796
explain extended select * from loc_orc;

-- column stats are not COMPLETE, so stats are not updated
-- numRows: 8 rawDataSize: 796
explain extended select * from loc_orc where state='OH';

analyze table loc_orc compute statistics for columns state,locid,zip,year;

-- state column has 5 distincts. numRows/countDistincts
-- numRows: 1 rawDataSize: 102
explain extended select * from loc_orc where state='OH';

-- not equals comparison shouldn't affect number of rows
-- numRows: 8 rawDataSize: 804
explain extended select * from loc_orc where state!='OH';
explain extended select * from loc_orc where state<>'OH';

-- nulls are treated as constant equality comparison
-- numRows: 1 rawDataSize: 102
explain extended select * from loc_orc where zip is null;
-- numRows: 1 rawDataSize: 102
explain extended select * from loc_orc where !(zip is not null);

-- not nulls are treated as inverse of nulls
-- numRows: 7 rawDataSize: 702
explain extended select * from loc_orc where zip is not null;
-- numRows: 7 rawDataSize: 702
explain extended select * from loc_orc where !(zip is null);

-- NOT evaluation. true will pass all rows, false will not pass any rows
-- numRows: 8 rawDataSize: 804
explain extended select * from loc_orc where !false;
-- numRows: 0 rawDataSize: 0
explain extended select * from loc_orc where !true;

-- OR evaluation. 1 row for OH and 1 row for CA
-- numRows: 2 rawDataSize: 204
explain extended select * from loc_orc where state='OH' or state='CA';

-- AND evaluation. cascadingly apply rules. 8/2 = 4/2 = 2
-- numRows: 2 rawDataSize: 204
explain extended select * from loc_orc where year=2001 and year is null;
-- numRows: 1 rawDataSize: 102
explain extended select * from loc_orc where year=2001 and state='OH' and state='FL';

-- AND and OR together. left expr will yield 1 row and right will yield 1 row
-- numRows: 3 rawDataSize: 306
explain extended select * from loc_orc where (year=2001 and year is null) or (state='CA');

-- AND and OR together. left expr will yield 8 rows and right will yield 1 row
-- numRows: 1 rawDataSize: 102
explain extended select * from loc_orc where (year=2001 or year is null) and (state='CA');

-- all inequality conditions rows/3 is the rules
-- numRows: 2 rawDataSize: 204
explain extended select * from loc_orc where locid < 30;
explain extended select * from loc_orc where locid > 30;
explain extended select * from loc_orc where locid <= 30;
explain extended select * from loc_orc where locid >= 30;
