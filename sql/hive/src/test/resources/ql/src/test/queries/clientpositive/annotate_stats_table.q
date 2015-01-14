set hive.stats.fetch.column.stats=true;
set hive.stats.autogather=false;

create table if not exists emp_staging (
  lastname string,
  deptid int
) row format delimited fields terminated by '|' stored as textfile;

create table if not exists emp_orc like emp_staging;
alter table emp_orc set fileformat orc;

-- basicStatState: NONE colStatState: NONE
explain extended select * from emp_orc;

LOAD DATA LOCAL INPATH '../../data/files/emp.txt' OVERWRITE INTO TABLE emp_staging;

insert overwrite table emp_orc select * from emp_staging;

-- stats are disabled. basic stats will report the file size but not raw data size. so initial statistics will be PARTIAL

-- basicStatState: PARTIAL colStatState: NONE
explain extended select * from emp_orc;

-- table level analyze statistics
analyze table emp_orc compute statistics;

-- basicStatState: COMPLETE colStatState: NONE
explain extended select * from emp_orc;

-- column level partial statistics
analyze table emp_orc compute statistics for columns deptid;

-- basicStatState: COMPLETE colStatState: PARTIAL
explain extended select * from emp_orc;

-- all selected columns have statistics
-- basicStatState: COMPLETE colStatState: COMPLETE
explain extended select deptid from emp_orc;

-- column level complete statistics
analyze table emp_orc compute statistics for columns lastname,deptid;

-- basicStatState: COMPLETE colStatState: COMPLETE
explain extended select * from emp_orc;

-- basicStatState: COMPLETE colStatState: COMPLETE
explain extended select lastname from emp_orc;

-- basicStatState: COMPLETE colStatState: COMPLETE
explain extended select deptid from emp_orc;

-- basicStatState: COMPLETE colStatState: COMPLETE
explain extended select lastname,deptid from emp_orc;
