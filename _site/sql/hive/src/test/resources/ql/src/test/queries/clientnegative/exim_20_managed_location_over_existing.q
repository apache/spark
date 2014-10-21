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

dfs ${system:test.dfs.mkdir} ../build/ql/test/data/tablestore/exim_department/temp;
dfs -rmr ../build/ql/test/data/tablestore/exim_department;

create table exim_department ( dep_id int comment "department id") 	
	stored as textfile
	location 'ql/test/data/tablestore/exim_department'
	tblproperties("creator"="krishna");
import table exim_department from 'ql/test/data/exports/exim_department'
	location 'ql/test/data/tablestore2/exim_department';
dfs -rmr ../build/ql/test/data/exports/exim_department;
drop table exim_department;
dfs -rmr ../build/ql/test/data/tablestore/exim_department;


drop database importer;
