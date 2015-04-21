set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department,exim_employee;

create table exim_employee ( emp_id int) partitioned by (emp_country string);
load data local inpath "../../data/files/test.dat" into table exim_employee partition (emp_country="in");		

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/exim_employee/emp_country=in/_logs;
dfs -touchz ${system:test.warehouse.dir}/exim_employee/emp_country=in/_logs/job.xml;
export table exim_employee to 'ql/test/data/exports/exim_employee';
drop table exim_employee;

create database importer;
use importer;

import from 'ql/test/data/exports/exim_employee';
describe formatted exim_employee;
select * from exim_employee;
dfs -rmr target/tmp/ql/test/data/exports/exim_employee;
drop table exim_employee;
drop database importer;
use default;
