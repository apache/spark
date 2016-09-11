drop table varchar_2;

create table varchar_2 (
  key varchar(10),
  value varchar(20)
);

insert overwrite table varchar_2 select * from src;

select value, sum(cast(key as int)), count(*) numrows
from src
group by value
order by value asc
limit 5;

-- should match the query from src
select value, sum(cast(key as int)), count(*) numrows
from varchar_2
group by value
order by value asc
limit 5;

select value, sum(cast(key as int)), count(*) numrows
from src
group by value
order by value desc
limit 5;

-- should match the query from src
select value, sum(cast(key as int)), count(*) numrows
from varchar_2
group by value
order by value desc
limit 5;

drop table varchar_2;
