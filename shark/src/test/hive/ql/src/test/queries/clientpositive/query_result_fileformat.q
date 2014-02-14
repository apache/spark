create table nzhang_test1 stored as sequencefile as select 'key1' as key, 'value
1

http://asdf' value from src limit 1;

select * from nzhang_test1;
select count(*) from nzhang_test1;

explain
select * from nzhang_test1 where key='key1';

select * from nzhang_test1 where key='key1';

set hive.query.result.fileformat=SequenceFile;

select * from nzhang_test1;

select count(*) from nzhang_test1;

explain
select * from nzhang_test1 where key='key1';

select * from nzhang_test1 where key='key1';
