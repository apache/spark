set hive.stats.autogather=true;

CREATE TABLE stats_invalid (key string, value string);

insert overwrite table stats_invalid
select * from src;

analyze table stats_invalid compute statistics for columns key,value;

desc formatted stats_invalid;
alter table stats_invalid add  columns (new_col string);

desc formatted stats_invalid;
drop table stats_invalid;

