set hive.stats.fetch.column.stats=true;

create table if not exists emp_staging (
  lastname string,
  deptid int
) row format delimited fields terminated by '|' stored as textfile;

create table if not exists dept_staging (
  deptid int,
  deptname string
) row format delimited fields terminated by '|' stored as textfile;

create table if not exists loc_staging (
  state string,
  locid int,
  zip bigint,
  year int
) row format delimited fields terminated by '|' stored as textfile;

create table if not exists emp_orc like emp_staging;
alter table emp_orc set fileformat orc;

create table if not exists dept_orc like dept_staging;
alter table dept_orc set fileformat orc;

create table loc_orc like loc_staging;
alter table loc_orc set fileformat orc;

LOAD DATA LOCAL INPATH '../../data/files/emp.txt' OVERWRITE INTO TABLE emp_staging;
LOAD DATA LOCAL INPATH '../../data/files/dept.txt' OVERWRITE INTO TABLE dept_staging;
LOAD DATA LOCAL INPATH '../../data/files/loc.txt' OVERWRITE INTO TABLE loc_staging;

insert overwrite table emp_orc select * from emp_staging;
insert overwrite table dept_orc select * from dept_staging;
insert overwrite table loc_orc select * from loc_staging;

analyze table emp_orc compute statistics for columns lastname,deptid;
analyze table dept_orc compute statistics for columns deptname,deptid;
analyze table loc_orc compute statistics for columns state,locid,zip,year;

-- number of rows
-- emp_orc  - 6
-- dept_orc - 4
-- loc_orc  - 8

-- count distincts for relevant columns (since count distinct values are approximate in some cases count distint values will be greater than number of rows)
-- emp_orc.deptid - 3
-- emp_orc.lastname - 7
-- dept_orc.deptid - 6
-- dept_orc.deptname - 5
-- loc_orc.locid - 6
-- loc_orc.state - 7

-- Expected output rows: 4
-- Reason: #rows = (6*4)/max(3,6)
explain extended select * from emp_orc e join dept_orc d on (e.deptid = d.deptid);

-- 3 way join
-- Expected output rows: 4
-- Reason: #rows = (6*4*6)/max(3,6)*max(6,3)
explain extended select * from emp_orc e join dept_orc d on (e.deptid = d.deptid) join emp_orc e1 on (e.deptid = e1.deptid);

-- Expected output rows: 5
-- Reason: #rows = (6*4*8)/max(3,6)*max(6,6)
explain extended select * from emp_orc e join dept_orc d  on (e.deptid = d.deptid) join loc_orc l on (e.deptid = l.locid);

-- join keys of different types
-- Expected output rows: 4
-- Reason: #rows = (6*4*8)/max(3,6)*max(6,7)
explain extended select * from emp_orc e join dept_orc d  on (e.deptid = d.deptid) join loc_orc l on (e.deptid = l.state);

-- multi-attribute join
-- Expected output rows: 0
-- Reason: #rows = (6*4)/max(3,6)*max(7,5)
explain extended select * from emp_orc e join dept_orc d on (e.deptid = d.deptid and e.lastname = d.deptname);

-- 3 way and multi-attribute join
-- Expected output rows: 0
-- Reason: #rows = (6*4*8)/max(3,6)*max(7,5)*max(3,6)*max(7,7)
explain extended select * from emp_orc e join dept_orc d on (e.deptid = d.deptid and e.lastname = d.deptname) join loc_orc l on (e.deptid = l.locid and e.lastname = l.state);

