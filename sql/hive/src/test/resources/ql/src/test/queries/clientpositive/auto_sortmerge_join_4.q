-- small 2 part, 4 bucket & big 1 part, 2 bucket
CREATE TABLE bucket_small (key string, value string) partitioned by (ds string) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../data/files/smallsrcsortbucket1outof4.txt' INTO TABLE bucket_small partition(ds='2008-04-08');
load data local inpath '../data/files/smallsrcsortbucket2outof4.txt' INTO TABLE bucket_small partition(ds='2008-04-08');
load data local inpath '../data/files/smallsrcsortbucket3outof4.txt' INTO TABLE bucket_small partition(ds='2008-04-08');
load data local inpath '../data/files/smallsrcsortbucket4outof4.txt' INTO TABLE bucket_small partition(ds='2008-04-08');

load data local inpath '../data/files/smallsrcsortbucket1outof4.txt' INTO TABLE bucket_small partition(ds='2008-04-09');
load data local inpath '../data/files/smallsrcsortbucket2outof4.txt' INTO TABLE bucket_small partition(ds='2008-04-09');
load data local inpath '../data/files/smallsrcsortbucket3outof4.txt' INTO TABLE bucket_small partition(ds='2008-04-09');
load data local inpath '../data/files/smallsrcsortbucket4outof4.txt' INTO TABLE bucket_small partition(ds='2008-04-09');

CREATE TABLE bucket_big (key string, value string) partitioned by (ds string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
load data local inpath '../data/files/srcsortbucket1outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-08');
load data local inpath '../data/files/srcsortbucket2outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-08');

set hive.auto.convert.join=true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.auto.convert.sortmerge.join.to.mapjoin=false;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.AvgPartitionSizeBasedBigTableSelectorForAutoSMJ;

-- Since size is being used to find the big table, the order of the tables in the join does not matter
explain extended select count(*) FROM bucket_small a JOIN bucket_big b ON a.key = b.key;
select count(*) FROM bucket_small a JOIN bucket_big b ON a.key = b.key;

explain extended select count(*) FROM bucket_big a JOIN bucket_small b ON a.key = b.key;
select count(*) FROM bucket_big a JOIN bucket_small b ON a.key = b.key;

set hive.auto.convert.sortmerge.join.to.mapjoin=true;
explain extended select count(*) FROM bucket_big a JOIN bucket_small b ON a.key = b.key;
select count(*) FROM bucket_big a JOIN bucket_small b ON a.key = b.key;
