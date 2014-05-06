-- small 1 part, 2 bucket & big 2 part, 4 bucket

CREATE TABLE bucket_small (key string, value string) partitioned by (ds string)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
load data local inpath '../data/files/smallsrcsortbucket1outof4.txt' INTO TABLE bucket_small partition(ds='2008-04-08');
load data local inpath '../data/files/smallsrcsortbucket2outof4.txt' INTO TABLE bucket_small partition(ds='2008-04-08');

CREATE TABLE bucket_big (key string, value string) partitioned by (ds string) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../data/files/srcsortbucket1outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-08');
load data local inpath '../data/files/srcsortbucket2outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-08');
load data local inpath '../data/files/srcsortbucket3outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-08');
load data local inpath '../data/files/srcsortbucket4outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-08');

load data local inpath '../data/files/srcsortbucket1outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-09');
load data local inpath '../data/files/srcsortbucket2outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-09');
load data local inpath '../data/files/srcsortbucket3outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-09');
load data local inpath '../data/files/srcsortbucket4outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-09');

set hive.auto.convert.join=true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;

CREATE TABLE bucket_medium (key string, value string) partitioned by (ds string)
CLUSTERED BY (key) SORTED BY (key) INTO 3 BUCKETS STORED AS TEXTFILE;
load data local inpath '../data/files/smallsrcsortbucket1outof4.txt' INTO TABLE bucket_medium partition(ds='2008-04-08');
load data local inpath '../data/files/smallsrcsortbucket2outof4.txt' INTO TABLE bucket_medium partition(ds='2008-04-08');
load data local inpath '../data/files/smallsrcsortbucket3outof4.txt' INTO TABLE bucket_medium partition(ds='2008-04-08');

explain extended select count(*) FROM bucket_small a JOIN bucket_medium b ON a.key = b.key JOIN bucket_big c ON c.key = b.key JOIN bucket_medium d ON c.key = b.key;
select count(*) FROM bucket_small a JOIN bucket_medium b ON a.key = b.key JOIN bucket_big c ON c.key = b.key JOIN bucket_medium d ON c.key = b.key;
