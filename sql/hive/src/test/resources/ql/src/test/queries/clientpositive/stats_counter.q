set hive.stats.dbclass=counter;
set hive.stats.autogather=false;

-- by analyze
create table dummy1 as select * from src;

analyze table dummy1 compute statistics;
desc formatted dummy1;

set hive.stats.dbclass=counter;
set hive.stats.autogather=true;

-- by autogather
create table dummy2 as select * from src;

desc formatted dummy2;
