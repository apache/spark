set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department,exim_employee;

create table exim_employee ( emp_id int comment "employee id") 	
	comment "employee table"
	partitioned by (emp_country string comment "two char iso code", emp_state string comment "free text")
	stored as textfile	
	tblproperties("creator"="krishna");
load data local inpath "../../data/files/test.dat" 
	into table exim_employee partition (emp_country="in", emp_state="tn");
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_employee/temp;
dfs -rmr target/tmp/ql/test/data/exports/exim_employee;
export table exim_employee to 'ql/test/data/exports/exim_employee';
drop table exim_employee;

create database importer;
use importer;
create table exim_employee ( emp_id int comment "employee id") 	
	comment "employee table"
	partitioned by (emp_country string comment "two char iso code", emp_state string comment "free text")
	stored as textfile	
	tblproperties("creator"="krishna");

set hive.security.authorization.enabled=true;
grant Alter on table exim_employee to user hive_test_user;
grant Update on table exim_employee to user hive_test_user;
import from 'ql/test/data/exports/exim_employee';

set hive.security.authorization.enabled=false;
select * from exim_employee;
dfs -rmr target/tmp/ql/test/data/exports/exim_employee;
drop table exim_employee;
drop database importer;
