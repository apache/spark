set hive.fetch.task.conversion=more;

-- test for NaN (not-a-number)
create table tstTbl1(n double);

insert overwrite table tstTbl1
select 'NaN' from src tablesample (1 rows);

select * from tstTbl1;

select round(n, 1) from tstTbl1;
select round(n) from tstTbl1;

-- test for Infinity
select round(1/0), round(1/0, 2), round(1.0/0.0), round(1.0/0.0, 2) from src tablesample (1 rows);
