create table hive_test_src ( col1 string ) partitioned by (pcol1 string) stored as textfile;
load data local inpath '../../data/files/test.dat' into table hive_test_src;
