-- test cases for collation support

-- Create a test table with data
create table t1(ucs_basic string collate 'ucs_basic', ucs_basic_lcase string collate 'ucs_basic_lcase') using parquet;
insert into t1 values('aaa', 'aaa');
insert into t1 values('AAA', 'AAA');
insert into t1 values('bbb', 'bbb');
insert into t1 values('BBB', 'BBB');

-- group by and count ucs_basic
select count(*) from t1 group by ucs_basic;

-- group by and count ucs_basic_lcase
select count(*) from t1 group by ucs_basic_lcase;

-- filter equal ucs_basic
select * from t1 where ucs_basic = 'aaa';

-- filter equal ucs_basic_lcase
select * from t1 where ucs_basic_lcase = 'aaa' collate 'ucs_basic_lcase';

-- filter less then ucs_basic
select * from t1 where ucs_basic < 'bbb';

-- filter less then ucs_basic_lcase
select * from t1 where ucs_basic_lcase < 'bbb' collate 'ucs_basic_lcase';

drop table t1;

-- create table with struct field
create table t1 (c1 struct<ucs_basic: string collate 'ucs_basic', ucs_basic_lcase: string collate 'ucs_basic_lcase'>) USING PARQUET;

INSERT INTO t1 VALUES (named_struct('ucs_basic', 'aaa', 'ucs_basic_lcase', 'aaa'));
INSERT INTO t1 VALUES (named_struct('ucs_basic', 'AAA', 'ucs_basic_lcase', 'AAA'));

-- aggregate against nested field ucs_basic
select count(*) from t1 group by c1.ucs_basic;

-- aggregate against nested field ucs_basic_lcase
select count(*) from t1 group by c1.ucs_basic_lcase;

drop table t1;
