set hive.test.mode=true;
set hive.test.mode.prefix=;

create table exim_department ( dep_id int comment "department id") 	
	clustered by (dep_id) sorted by (dep_id desc) into 10 buckets
	stored by "org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler"	
	tblproperties("creator"="krishna");
export table exim_department to 'ql/test/data/exports/exim_department';	
drop table exim_department;