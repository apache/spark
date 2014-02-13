


create table tmp_insert_test (key string, value string) stored as textfile;
load data local inpath '../data/files/kv1.txt' into table tmp_insert_test;
select * from tmp_insert_test;

create table tmp_insert_test_p (key string, value string) partitioned by (ds string) stored as textfile;

load data local inpath '../data/files/kv1.txt' into table tmp_insert_test_p partition (ds = '2009-08-01');
select * from tmp_insert_test_p where ds= '2009-08-01'
order by key, value;

load data local inpath '../data/files/kv2.txt' into table tmp_insert_test_p partition (ds = '2009-08-01');
select * from tmp_insert_test_p where ds= '2009-08-01'
order by key, value;
