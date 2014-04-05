set datanucleus.cache.collections=false;

create table stats_src like src;
insert overwrite table stats_src select * from src;
analyze table stats_src compute statistics;
desc formatted stats_src;

create table stats_part like srcpart;

insert overwrite table stats_part partition (ds='2010-04-08', hr = '11') select key, value from src;
insert overwrite table stats_part partition (ds='2010-04-08', hr = '12') select key, value from src;

analyze table stats_part partition(ds='2010-04-08', hr='11') compute statistics;
analyze table stats_part partition(ds='2010-04-08', hr='12') compute statistics;

insert overwrite table stats_part partition (ds='2010-04-08', hr = '13') select key, value from src;

desc formatted stats_part;
desc formatted stats_part partition (ds='2010-04-08', hr = '11');
desc formatted stats_part partition (ds='2010-04-08', hr = '12');

analyze table stats_part partition(ds, hr) compute statistics;
desc formatted stats_part;

drop table stats_src;
drop table stats_part;