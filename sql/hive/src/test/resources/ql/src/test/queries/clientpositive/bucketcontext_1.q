-- small 1 part, 2 bucket & big 2 part, 4 bucket
CREATE TABLE bucket_small (key string, value string) partitioned by (ds string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
load data local inpath '../data/files/srcsortbucket1outof4.txt' INTO TABLE bucket_small partition(ds='2008-04-08');
load data local inpath '../data/files/srcsortbucket2outof4.txt' INTO TABLE bucket_small partition(ds='2008-04-08');

CREATE TABLE bucket_big (key string, value string) partitioned by (ds string) CLUSTERED BY (key) SORTED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
load data local inpath '../data/files/srcsortbucket1outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-08');
load data local inpath '../data/files/srcsortbucket2outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-08');
load data local inpath '../data/files/srcsortbucket3outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-08');
load data local inpath '../data/files/srcsortbucket4outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-08');

load data local inpath '../data/files/srcsortbucket1outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-09');
load data local inpath '../data/files/srcsortbucket2outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-09');
load data local inpath '../data/files/srcsortbucket3outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-09');
load data local inpath '../data/files/srcsortbucket4outof4.txt' INTO TABLE bucket_big partition(ds='2008-04-09');

set hive.optimize.bucketmapjoin = true;
explain extended select /* + MAPJOIN(a) */ count(*) FROM bucket_small a JOIN bucket_big b ON a.key = b.key;
select /* + MAPJOIN(a) */ count(*) FROM bucket_small a JOIN bucket_big b ON a.key = b.key;

set hive.optimize.bucketmapjoin.sortedmerge = true;
explain extended select /* + MAPJOIN(a) */ count(*) FROM bucket_small a JOIN bucket_big b ON a.key = b.key;
select /* + MAPJOIN(a) */ count(*) FROM bucket_small a JOIN bucket_big b ON a.key = b.key;
