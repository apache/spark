set hive.test.mode=true;
set hive.test.mode.prefix=;

create table exim_department ( dep_id int comment "department id") 	
	stored as textfile	
	tblproperties("creator"="krishna");
load data local inpath "../data/files/test.dat" into table exim_department;	
dfs ${system:test.dfs.mkdir} ../build/ql/test/data/exports/exim_department/temp;
dfs -rmr ../build/ql/test/data/exports/exim_department;
export table exim_department to 'ql/test/data/exports/exim_department';
drop table exim_department;

create database importer;
use importer;

create table exim_department ( dep_id int comment "department id") 	
	row format serde "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
		with serdeproperties ("serialization.format"="0")
	stored as inputformat "org.apache.hadoop.mapred.TextInputFormat" 
		outputformat "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat" 
		inputdriver "org.apache.hadoop.hive.howl.rcfile.RCFileInputDriver" 
		outputdriver "org.apache.hadoop.hive.howl.rcfile.RCFileOutputDriver"
	tblproperties("creator"="krishna");
import from 'ql/test/data/exports/exim_department';
drop table exim_department;
dfs -rmr ../build/ql/test/data/exports/exim_department;

drop database importer;
