set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department,exim_employee;

create table exim_employee ( emp_id int comment "employee id") 	
	comment "employee table"
	partitioned by (emp_country string comment "two char iso code", emp_state string comment "free text")
	stored as textfile	
	tblproperties("creator"="krishna");
load data local inpath "../data/files/test.dat" 
	into table exim_employee partition (emp_country="in", emp_state="tn");	
load data local inpath "../data/files/test.dat" 
	into table exim_employee partition (emp_country="in", emp_state="ka");	
load data local inpath "../data/files/test.dat" 
	into table exim_employee partition (emp_country="us", emp_state="tn");	
load data local inpath "../data/files/test.dat" 
	into table exim_employee partition (emp_country="us", emp_state="ka");		
dfs ${system:test.dfs.mkdir} ../build/ql/test/data/exports/exim_employee/temp;
dfs -rmr ../build/ql/test/data/exports/exim_employee;
export table exim_employee to 'ql/test/data/exports/exim_employee';
drop table exim_employee;

create database importer;
use importer;

dfs ${system:test.dfs.mkdir} ../build/ql/test/data/tablestore/exim_employee/temp;
dfs -rmr ../build/ql/test/data/tablestore/exim_employee;

create table exim_employee ( emp_id int comment "employee id") 	
	comment "employee table"
	partitioned by (emp_country string comment "two char iso code", emp_state string comment "free text")
	stored as textfile	
	tblproperties("creator"="krishna");
import table exim_employee partition (emp_country="us", emp_state="tn") 
	from 'ql/test/data/exports/exim_employee'
	location 'ql/test/data/tablestore/exim_employee';
alter table exim_employee add partition	(emp_country="us", emp_state="ap")
	location 'ql/test/data/tablestore2/exim_employee';
show table extended like exim_employee;
show table extended like exim_employee partition (emp_country="us", emp_state="tn");
show table extended like exim_employee partition (emp_country="us", emp_state="ap");	
dfs -rmr ../build/ql/test/data/exports/exim_employee;
select * from exim_employee;
dfs -rmr ../build/ql/test/data/tablestore/exim_employee;
select * from exim_employee;
drop table exim_employee;

drop database importer;
