-- HIVE-3849 Aliased column in where clause for multi-groupby single reducer cannot be resolved
create table e1 (key string, count int);
create table e2 (key string, count int);

explain
from src
insert overwrite table e1
select key, count(*)
where src.value in ('val_100', 'val_200', 'val_300') AND key in (100, 150, 200)
group by key
insert overwrite table e2
select key, count(*)
where src.value in ('val_400', 'val_500') AND key in (400, 450)
group by key;

from src
insert overwrite table e1
select key, count(*)
where src.value in ('val_100', 'val_200', 'val_300') AND key in (100, 150, 200)
group by key
insert overwrite table e2
select key, count(*)
where src.value in ('val_400', 'val_500') AND key in (400, 450)
group by key;

select * from e1;
select * from e2;

explain
from src
insert overwrite table e1
select value, count(*)
where src.key + src.key = 200 or src.key - 100 = 100 or src.key = 300 AND VALUE IS NOT NULL
group by value
insert overwrite table e2
select value, count(*)
where src.key + src.key = 400 or src.key - 100 = 500 AND VALUE IS NOT NULL
group by value;

from src
insert overwrite table e1
select value, count(*)
where src.key + src.key = 200 or src.key - 100 = 100 or src.key = 300 AND VALUE IS NOT NULL
group by value
insert overwrite table e2
select value, count(*)
where src.key + src.key = 400 or src.key - 100 = 500 AND VALUE IS NOT NULL
group by value;

select * from e1;
select * from e2;

set hive.optimize.ppd=false;

explain
from src
insert overwrite table e1
select key, count(*)
where src.value in ('val_100', 'val_200', 'val_300') AND key in (100, 150, 200)
group by key
insert overwrite table e2
select key, count(*)
where src.value in ('val_400', 'val_500') AND key in (400, 450)
group by key;

from src
insert overwrite table e1
select key, count(*)
where src.value in ('val_100', 'val_200', 'val_300') AND key in (100, 150, 200)
group by key
insert overwrite table e2
select key, count(*)
where src.value in ('val_400', 'val_500') AND key in (400, 450)
group by key;

select * from e1;
select * from e2;

explain
from src
insert overwrite table e1
select value, count(*)
where src.key + src.key = 200 or src.key - 100 = 100 or src.key = 300 AND VALUE IS NOT NULL
group by value
insert overwrite table e2
select value, count(*)
where src.key + src.key = 400 or src.key - 100 = 500 AND VALUE IS NOT NULL
group by value;

from src
insert overwrite table e1
select value, count(*)
where src.key + src.key = 200 or src.key - 100 = 100 or src.key = 300 AND VALUE IS NOT NULL
group by value
insert overwrite table e2
select value, count(*)
where src.key + src.key = 400 or src.key - 100 = 500 AND VALUE IS NOT NULL
group by value;

select * from e1;
select * from e2;
