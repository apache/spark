drop table test1;
drop table test2;
drop table test3;
drop table test4;

create table test1 (key string, value string) clustered by (key) sorted by (key) into 3 buckets;
create table test2 (key string, value string) clustered by (value) sorted by (value) into 3 buckets;
create table test3 (key string, value string) clustered by (key, value) sorted by (key, value) into 3 buckets;
create table test4 (key string, value string) clustered by (value, key) sorted by (value, key) into 3 buckets;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE test1;
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE test1;
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE test1;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE test2;
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE test2;
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE test2;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE test3;
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE test3;
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE test3;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE test4;
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE test4;
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE test4;

set hive.optimize.bucketmapjoin = true;
-- should be allowed
explain extended select /* + MAPJOIN(R) */ * from test1 L join test1 R on L.key=R.key AND L.value=R.value;
explain extended select /* + MAPJOIN(R) */ * from test2 L join test2 R on L.key=R.key AND L.value=R.value;

-- should not apply bucket mapjoin
explain extended select /* + MAPJOIN(R) */ * from test1 L join test1 R on L.key+L.key=R.key;
explain extended select /* + MAPJOIN(R) */ * from test1 L join test2 R on L.key=R.key AND L.value=R.value;
explain extended select /* + MAPJOIN(R) */ * from test1 L join test3 R on L.key=R.key AND L.value=R.value;
explain extended select /* + MAPJOIN(R) */ * from test1 L join test4 R on L.key=R.key AND L.value=R.value;
explain extended select /* + MAPJOIN(R) */ * from test2 L join test3 R on L.key=R.key AND L.value=R.value;
explain extended select /* + MAPJOIN(R) */ * from test2 L join test4 R on L.key=R.key AND L.value=R.value;
explain extended select /* + MAPJOIN(R) */ * from test3 L join test4 R on L.key=R.key AND L.value=R.value;
