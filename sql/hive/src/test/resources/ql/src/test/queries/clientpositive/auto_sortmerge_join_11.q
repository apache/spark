-- small 1 part, 2 bucket & big 2 part, 4 bucket

CREATE TABLE bucket_small (key string, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/smallsrcsortbucket1outof4.txt' INTO TABLE bucket_small partition(ds='2008-04-08');
load data local inpath '../../data/files/smallsrcsortbucket2outof4.txt' INTO TABLE bucket_small partition(ds='2008-04-08');

CREATE TABLE bucket_big (key string, value string) partitioned by (ds string) CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../../data/files/srcsortbucket1outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-08');
load data local inpath '../../data/files/srcsortbucket2outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-08');
load data local inpath '../../data/files/srcsortbucket3outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-08');
load data local inpath '../../data/files/srcsortbucket4outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-08');

load data local inpath '../../data/files/srcsortbucket1outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-09');
load data local inpath '../../data/files/srcsortbucket2outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-09');
load data local inpath '../../data/files/srcsortbucket3outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-09');
load data local inpath '../../data/files/srcsortbucket4outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-09');

set hive.auto.convert.join=true;

explain extended select count(*) FROM bucket_small a JOIN bucket_big b ON a.key = b.key;
select count(*) FROM bucket_small a JOIN bucket_big b ON a.key = b.key;

set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin=true;
set hive.optimize.bucketmapjoin.sortedmerge=true;

-- Since size is being used to find the big table, the order of the tables in the join does not matter
-- The tables are only bucketed and not sorted, the join should not be converted
-- Currenly, a join is only converted to a sort-merge join without a hint, automatic conversion to
-- bucketized mapjoin is not done
explain extended select count(*) FROM bucket_small a JOIN bucket_big b ON a.key = b.key;
select count(*) FROM bucket_small a JOIN bucket_big b ON a.key = b.key;

-- The join is converted to a bucketed mapjoin with a mapjoin hint
explain extended select /*+ mapjoin(a) */ count(*) FROM bucket_small a JOIN bucket_big b ON a.key = b.key;
select /*+ mapjoin(a) */ count(*) FROM bucket_small a JOIN bucket_big b ON a.key = b.key;
