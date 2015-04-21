CREATE TABLE srcbucket_mapjoin_part (key int, value string) 
  partitioned by (ds string) CLUSTERED BY (key) INTO 3 BUCKETS
  STORED AS TEXTFILE;
load data local inpath '../../data/files/srcbucket20.txt' 
  INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket21.txt' 
  INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket22.txt' 
  INTO TABLE srcbucket_mapjoin_part partition(ds='2008-04-08');

CREATE TABLE srcbucket_mapjoin_part_2 (key int, value string)
  partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS
  STORED AS TEXTFILE;
load data local inpath '../../data/files/srcbucket22.txt'
  INTO TABLE srcbucket_mapjoin_part_2 partition(ds='2008-04-08');
load data local inpath '../../data/files/srcbucket23.txt'
  INTO TABLE srcbucket_mapjoin_part_2 partition(ds='2008-04-08');

-- The number of buckets in the 2 tables above (being joined later) dont match.
-- Throw an error if the user requested a bucketed mapjoin to be enforced.
-- In the default case (hive.enforce.bucketmapjoin=false), the query succeeds 
-- even though mapjoin is not being performed

explain
select a.key, a.value, b.value 
from srcbucket_mapjoin_part a join srcbucket_mapjoin_part_2 b
on a.key=b.key and a.ds="2008-04-08" and b.ds="2008-04-08";

set hive.optimize.bucketmapjoin = true;

explain
select /*+mapjoin(b)*/ a.key, a.value, b.value 
from srcbucket_mapjoin_part a join srcbucket_mapjoin_part_2 b
on a.key=b.key and a.ds="2008-04-08" and b.ds="2008-04-08";

set hive.enforce.bucketmapjoin=true;

explain
select /*+mapjoin(b)*/ a.key, a.value, b.value 
from srcbucket_mapjoin_part a join srcbucket_mapjoin_part_2 b
on a.key=b.key and a.ds="2008-04-08" and b.ds="2008-04-08";

