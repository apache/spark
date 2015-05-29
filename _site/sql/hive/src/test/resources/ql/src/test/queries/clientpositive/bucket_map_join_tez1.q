set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

CREATE TABLE srcbucket_mapjoin(key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
CREATE TABLE tab_part (key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
CREATE TABLE srcbucket_mapjoin_part (key int, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin partition(ds='2008-04-08');

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket23.txt' INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');

set hive.enforce.bucketing=true;
set hive.enforce.sorting = true;
set hive.optimize.bucketingsorting=false;
insert overwrite table tab_part partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin_part;

CREATE TABLE tab(key int, value string) PARTITIONED BY(ds STRING) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab partition (ds='2008-04-08')
select key,value from srcbucket_mapjoin;

set hive.convert.join.bucket.mapjoin.tez = true;
explain
select a.key, a.value, b.value
from tab a join tab_part b on a.key = b.key;

-- one side is really bucketed. srcbucket_mapjoin is not really a bucketed table.
-- In this case the sub-query is chosen as the big table.
explain
select a.k1, a.v1, b.value
from (select sum(substr(srcbucket_mapjoin.value,5)) as v1, key as k1 from srcbucket_mapjoin GROUP BY srcbucket_mapjoin.key) a
join tab b on a.k1 = b.key;

explain
select a.k1, a.v1, b.value
from (select sum(substr(tab.value,5)) as v1, key as k1 from tab_part join tab on tab_part.key = tab.key GROUP BY tab.key) a
join tab b on a.k1 = b.key;

explain
select a.k1, a.v1, b.value
from (select sum(substr(x.value,5)) as v1, x.key as k1 from tab x join tab y on x.key = y.key GROUP BY x.key) a
join tab_part b on a.k1 = b.key;

-- multi-way join
explain
select a.key, a.value, b.value
from tab_part a join tab b on a.key = b.key join tab c on a.key = c.key;

explain
select a.key, a.value, c.value
from (select x.key, x.value from tab_part x join tab y on x.key = y.key) a join tab c on a.key = c.key;

-- in this case sub-query is the small table
explain
select a.key, a.value, b.value
from (select key, sum(substr(srcbucket_mapjoin.value,5)) as value from srcbucket_mapjoin GROUP BY srcbucket_mapjoin.key) a
join tab_part b on a.key = b.key;

set hive.map.aggr=false;
explain
select a.key, a.value, b.value
from (select key, sum(substr(srcbucket_mapjoin.value,5)) as value from srcbucket_mapjoin GROUP BY srcbucket_mapjoin.key) a
join tab_part b on a.key = b.key;

-- join on non-bucketed column results in broadcast join.
explain
select a.key, a.value, b.value
from tab a join tab_part b on a.value = b.value;

CREATE TABLE tab1(key int, value string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
insert overwrite table tab1
select key,value from srcbucket_mapjoin;

explain
select a.key, a.value, b.value
from tab1 a join tab_part b on a.key = b.key;

explain select a.key, b.key from tab_part a join tab_part c on a.key = c.key join tab_part b on a.value = b.value;


