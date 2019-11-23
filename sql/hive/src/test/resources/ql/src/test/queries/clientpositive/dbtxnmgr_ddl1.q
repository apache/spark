set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create database D1;

alter database D1 set dbproperties('test'='yesthisis');

drop database D1;

create table T1(key string, val string) stored as textfile;

create table T2 like T1;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;

select * from T1;

create table T3 as select * from T1;

create table T4 (key char(10), val decimal(5,2), b int)
    partitioned by (ds string)
    clustered by (b) into 10 buckets
    stored as orc;

alter table T3 rename to newT3;

alter table T2 set tblproperties ('test'='thisisatest');

alter table T2 set serde 'org.apache.hadoop.hive.ql.io.orc.OrcSerde';
alter table T2 set serdeproperties ('test'='thisisatest');

alter table T2 clustered by (key) into 32 buckets;

alter table T4 add partition (ds='today'); 

alter table T4 partition (ds='today') rename to partition(ds='yesterday');

alter table T4 drop partition (ds='yesterday');

alter table T4 add partition (ds='tomorrow'); 

create table T5 (a string, b int);
alter table T5 set fileformat orc;

create table T7 (a string, b int);
alter table T7 set location 'file:///tmp';

alter table T2 touch;
alter table T4 touch partition (ds='tomorrow');

create view V1 as select key from T1;
alter view V1 set tblproperties ('test'='thisisatest');
drop view V1;



drop table T1;
drop table T2;
drop table newT3;
