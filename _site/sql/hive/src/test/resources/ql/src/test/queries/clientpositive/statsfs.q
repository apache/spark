set hive.stats.dbclass=fs;

-- stats computation on partitioned table with analyze command

create table t1 (key string, value string) partitioned by (ds string);
load data local inpath '../../data/files/kv1.txt' into table t1 partition (ds = '2010');
load data local inpath '../../data/files/kv1.txt' into table t1 partition (ds = '2011');

analyze table t1 partition (ds) compute statistics;

describe formatted t1 partition (ds='2010');
describe formatted t1 partition (ds='2011');

drop table t1;

-- stats computation on partitioned table with autogather on insert query

create table t1 (key string, value string) partitioned by (ds string);

insert into table t1 partition (ds='2010') select * from src;
insert into table t1 partition (ds='2011') select * from src;

describe formatted t1 partition (ds='2010');
describe formatted t1 partition (ds='2011');

drop table t1;

-- analyze stmt on unpartitioned table

create table t1 (key string, value string); 
load data local inpath '../../data/files/kv1.txt' into table t1; 

analyze table t1 compute statistics;

describe formatted t1 ;

drop table t1;

-- stats computation on unpartitioned table with autogather on insert query

create table t1 (key string, value string); 

insert into table t1  select * from src;

describe formatted t1 ;

drop table t1;

-- stats computation on partitioned table with autogather on insert query with dynamic partitioning


create table t1 (key string, value string) partitioned by (ds string, hr string);

set hive.exec.dynamic.partition.mode=nonstrict;
insert into table t1 partition (ds,hr) select * from srcpart;

describe formatted t1 partition (ds='2008-04-08',hr='11');
describe formatted t1 partition (ds='2008-04-09',hr='12');

drop table t1;
set hive.exec.dynamic.partition.mode=strict;

set hive.stats.dbclass=jdbc:derby;
