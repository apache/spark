set datanucleus.cache.collections=false;
set datanucleus.cache.collections.lazy=false;

create database db1;

show databases;

create database db2 with dbproperties (
  'mapred.jobtracker.url'='http://my.jobtracker.com:53000',
  'hive.warehouse.dir' = '/user/hive/warehouse',
  'mapred.scratch.dir' = 'hdfs://tmp.dfs.com:50029/tmp');

describe database db2;

describe database extended db2;


set datanucleus.cache.collections=false;
set datanucleus.cache.collections.lazy=false;

alter database db2 set dbproperties (
  'new.property' = 'some new props',
  'hive.warehouse.dir' = 'new/warehouse/dir');

describe database extended db2;

