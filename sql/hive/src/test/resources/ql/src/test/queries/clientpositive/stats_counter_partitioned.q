set hive.stats.dbclass=counter;
set hive.stats.autogather=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- partitioned table analyze 

create table dummy (key string, value string) partitioned by (ds string, hr string);

load data local inpath '../../data/files/kv1.txt' into table dummy partition (ds='2008',hr='12');
load data local inpath '../../data/files/kv1.txt' into table dummy partition (ds='2008',hr='11');

analyze table dummy partition (ds,hr) compute statistics;
describe formatted dummy partition (ds='2008', hr='11');
describe formatted dummy partition (ds='2008', hr='12');

drop table dummy;

-- static partitioned table on insert

create table dummy (key string, value string) partitioned by (ds string, hr string);

insert overwrite table dummy partition (ds='10',hr='11') select * from src;
insert overwrite table dummy partition (ds='10',hr='12') select * from src;

describe formatted dummy partition (ds='10', hr='11');
describe formatted dummy partition (ds='10', hr='12');

drop table dummy;

-- dynamic partitioned table on insert

create table dummy (key int) partitioned by (hr int);
                                                                                                      
CREATE TABLE tbl(key int, value int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INPATH '../../data/files/tbl.txt' OVERWRITE INTO TABLE tbl;                           
                                                                                                      
insert overwrite table dummy partition (hr) select * from tbl;

describe formatted dummy partition (hr=1997);
describe formatted dummy partition (hr=1994);
describe formatted dummy partition (hr=1998);
describe formatted dummy partition (hr=1996);

drop table tbl;
drop table dummy; 
