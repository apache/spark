-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.20,0.20S)
create table tst1(key string, value string) partitioned by (ds string) clustered by (key) into 10 buckets;

alter table tst1 clustered by (key) into 8 buckets;

describe formatted tst1;

set hive.enforce.bucketing=true;
insert overwrite table tst1 partition (ds='1') select key, value from src;

describe formatted tst1 partition (ds = '1');

-- Test changing bucket number

alter table tst1 clustered by (key) into 12 buckets;

insert overwrite table tst1 partition (ds='1') select key, value from src;

describe formatted tst1 partition (ds = '1');

describe formatted tst1;

-- Test changing bucket number of (table/partition)

alter table tst1 into 4 buckets;

describe formatted tst1;

describe formatted tst1 partition (ds = '1');

alter table tst1 partition (ds = '1') into 6 buckets;

describe formatted tst1;

describe formatted tst1 partition (ds = '1');

-- Test adding sort order

alter table tst1 clustered by (key) sorted by (key asc) into 12 buckets;

describe formatted tst1;

-- Test changing sort order

alter table tst1 clustered by (key) sorted by (value desc) into 12 buckets;

describe formatted tst1;

-- Test removing test order

alter table tst1 clustered by (value) into 12 buckets;

describe formatted tst1;

-- Test removing buckets

alter table tst1 not clustered;

describe formatted tst1;
