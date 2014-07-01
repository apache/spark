set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

explain extended select a.key, b.k2, b.k3
from src a
join (
select key,
min(key) as k,
min(key)+1 as k1,
min(key)+2 as k2,
min(key)+3 as k3
from src
group by key
) b
on a.key=b.key and b.k1 < 5;

select a.key, b.k2, b.k3
from src a
join (
select key,
min(key) as k,
min(key)+1 as k1,
min(key)+2 as k2,
min(key)+3 as k3
from src
group by key
) b
on a.key=b.key and b.k1 < 5;

set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;

explain extended select a.key, b.k2, b.k3
from src a
join (
select key,
min(key) as k,
min(key)+1 as k1,
min(key)+2 as k2,
min(key)+3 as k3
from src
group by key
) b
on a.key=b.key and b.k1 < 5;

select a.key, b.k2, b.k3
from src a
join (
select key,
min(key) as k,
min(key)+1 as k1,
min(key)+2 as k2,
min(key)+3 as k3
from src
group by key
) b
on a.key=b.key and b.k1 < 5;

set hive.optimize.ppd=false;
set hive.ppd.remove.duplicatefilters=false;

explain extended select a.key, b.k2, b.k3
from src a
join (
select key,
min(key) as k,
min(key)+1 as k1,
min(key)+2 as k2,
min(key)+3 as k3
from src
group by key
) b
on a.key=b.key and b.k1 < 5;

select a.key, b.k2, b.k3
from src a
join (
select key,
min(key) as k,
min(key)+1 as k1,
min(key)+2 as k2,
min(key)+3 as k3
from src
group by key
) b
on a.key=b.key and b.k1 < 5;

set hive.optimize.ppd=faluse;
set hive.ppd.remove.duplicatefilters=true;

explain extended select a.key, b.k2, b.k3
from src a
join (
select key,
min(key) as k,
min(key)+1 as k1,
min(key)+2 as k2,
min(key)+3 as k3
from src
group by key
) b
on a.key=b.key and b.k1 < 5;

select a.key, b.k2, b.k3
from src a
join (
select key,
min(key) as k,
min(key)+1 as k1,
min(key)+2 as k2,
min(key)+3 as k3
from src
group by key
) b
on a.key=b.key and b.k1 < 5;

