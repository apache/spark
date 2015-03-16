
-- qtest_get_java_boolean should already be created during test initialization
select qtest_get_java_boolean('true'), qtest_get_java_boolean('false') from src limit 1;

create database mydb;
create function mydb.func1 as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper';

show functions mydb.func1;

select mydb.func1('abc') from src limit 1;

drop function mydb.func1;

-- function should now be gone
show functions mydb.func1;

-- To test function name resolution
create function mydb.qtest_get_java_boolean as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper';

use default;
-- unqualified function should resolve to one in default db
select qtest_get_java_boolean('abc'), default.qtest_get_java_boolean('abc'), mydb.qtest_get_java_boolean('abc') from default.src limit 1;

use mydb;
-- unqualified function should resolve to one in mydb db
select qtest_get_java_boolean('abc'), default.qtest_get_java_boolean('abc'), mydb.qtest_get_java_boolean('abc') from default.src limit 1;

drop function mydb.qtest_get_java_boolean;

drop database mydb cascade;
