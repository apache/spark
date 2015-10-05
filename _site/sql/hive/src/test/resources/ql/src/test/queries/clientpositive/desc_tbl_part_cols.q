create table t1 (a int, b string) partitioned by (c int, d string);
describe t1;

set hive.display.partition.cols.separately=false;
describe t1;

set hive.display.partition.cols.separately=true;
