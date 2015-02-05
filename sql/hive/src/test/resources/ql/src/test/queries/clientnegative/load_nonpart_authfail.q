create table hive_test_src ( col1 string ) stored as textfile;
set hive.security.authorization.enabled=true;
load data local inpath '../../data/files/test.dat' overwrite into table hive_test_src ;
