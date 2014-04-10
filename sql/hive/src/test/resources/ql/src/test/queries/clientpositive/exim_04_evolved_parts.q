set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=exim_department,exim_employee;

create table exim_employee (emp_id int comment 'employee id', emp_name string, emp_dob string comment 'employee date of birth', emp_sex string comment 'M/F') 
 comment 'employee table' 
 partitioned by (emp_country string comment '2-char code', emp_state string comment '2-char code')
 clustered by (emp_sex) sorted by (emp_id ASC) into 10 buckets
 row format serde "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe" with serdeproperties ('serialization.format'='1')
 stored as rcfile;

alter table exim_employee add columns (emp_dept int);
alter table exim_employee clustered by (emp_sex, emp_dept) sorted by (emp_id desc) into 5 buckets;
alter table exim_employee add partition (emp_country='in', emp_state='tn');

alter table exim_employee set serde "org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe" with serdeproperties ('serialization.format'='2');
alter table exim_employee set fileformat 
	inputformat "org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat" 
	outputformat "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat";

alter table exim_employee add partition (emp_country='in', emp_state='ka');
dfs ${system:test.dfs.mkdir} ../build/ql/test/data/exports/exim_employee/temp;
dfs -rmr ../build/ql/test/data/exports/exim_employee;
export table exim_employee to 'ql/test/data/exports/exim_employee';
drop table exim_employee;

create database importer;
use importer;

import from 'ql/test/data/exports/exim_employee';
describe extended exim_employee;
describe extended exim_employee partition (emp_country='in', emp_state='tn');
describe extended exim_employee partition (emp_country='in', emp_state='ka');
show table extended like exim_employee;
dfs -rmr ../build/ql/test/data/exports/exim_employee;
select * from exim_employee;
drop table exim_employee;

drop database importer;