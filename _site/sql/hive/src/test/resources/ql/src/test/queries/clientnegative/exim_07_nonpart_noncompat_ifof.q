set hive.test.mode=true;
set hive.test.mode.prefix=;

create table exim_department ( dep_id int comment "department id") 	
	stored as textfile	
	tblproperties("creator"="krishna");
load data local inpath "../../data/files/test.dat" into table exim_department;	
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/exim_department/temp;
dfs -rmr target/tmp/ql/test/data/exports/exim_department;
export table exim_department to 'ql/test/data/exports/exim_department';
drop table exim_department;

create database importer;
use importer;

create table exim_department ( dep_id int comment "department id") 	
	stored as inputformat "org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat" 
		outputformat "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat" 
		inputdriver "org.apache.hadoop.hive.howl.rcfile.RCFileInputDriver" 
		outputdriver "org.apache.hadoop.hive.howl.rcfile.RCFileOutputDriver" 	
	tblproperties("creator"="krishna");
import from 'ql/test/data/exports/exim_department';
drop table exim_department;
dfs -rmr target/tmp/ql/test/data/exports/exim_department;

drop database importer;
