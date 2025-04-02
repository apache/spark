create table hive_test_src ( col1 string ) partitioned by (pcol1 string) stored as textfile;
alter table hive_test_src add partition (pcol1 = 'test_part');
set hive.security.authorization.enabled=true;
grant Update on table hive_test_src to user hive_test_user;
load data local inpath '../../data/files/test.dat' overwrite into table hive_test_src partition (pcol1 = 'test_part');
