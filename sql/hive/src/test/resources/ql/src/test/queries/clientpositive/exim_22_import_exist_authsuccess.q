set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department,exim_employee;

create table exim_department ( dep_id int) stored as textfile;
load data local inpath "../data/files/test.dat" into table exim_department;
dfs ${system:test.dfs.mkdir} ../build/ql/test/data/exports/exim_department/temp;
dfs -rmr ../build/ql/test/data/exports/exim_department;
export table exim_department to 'ql/test/data/exports/exim_department';
drop table exim_department;

create database importer;
use importer;

create table exim_department ( dep_id int) stored as textfile;
set hive.security.authorization.enabled=true;
grant Alter on table exim_department to user hive_test_user;
grant Update on table exim_department to user hive_test_user;
import from 'ql/test/data/exports/exim_department';

set hive.security.authorization.enabled=false;
select * from exim_department;
drop table exim_department;
drop database importer;
dfs -rmr ../build/ql/test/data/exports/exim_department;

