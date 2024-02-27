-- test cases for collation support

-- Create a test table with data
create table t1(ucs_basic string collate 'ucs_basic', ucs_basic_lcase string collate 'ucs_basic_lcase') using parquet;
insert into t1 values('aaa', 'aaa');
insert into t1 values('AAA', 'AAA');
insert into t1 values('bbb', 'bbb');
insert into t1 values('BBB', 'BBB');

-- group by and count ucs_basic
select count(*), ucs_basic from t1 group by ucs_basic;

-- group by and count ucs_basic_lcase
select count(*), ucs_basic_lcase from t1 group by ucs_basic_lcase;

-- filter equal ucs_basic
select * from t1 where ucs_basic = 'aaa';

-- filter equal ucs_basic_lcase
select * from t1 where ucs_basic_lcase = 'aaa' collate 'ucs_basic_lcase';

-- filter less then ucs_basic
select * from t1 where ucs_basic < 'bbb';

-- filter less then ucs_basic_lcase
select * from t1 where ucs_basic_lcase < 'bbb' collate 'ucs_basic_lcase';

drop table t1
