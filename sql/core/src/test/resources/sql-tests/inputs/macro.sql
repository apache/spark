CREATE TEMPORARY MACRO SIGMOID (x DOUBLE) 1.0 / (1.0 + EXP(-x));
SELECT SIGMOID(2);
DROP TEMPORARY MACRO SIGMOID;

CREATE TEMPORARY MACRO FIXED_NUMBER() 1;
SELECT FIXED_NUMBER() + 1;
DROP TEMPORARY MACRO FIXED_NUMBER;

CREATE TEMPORARY MACRO SIMPLE_ADD (x INT, y INT) x + y;
SELECT SIMPLE_ADD(1, 9);
DROP TEMPORARY MACRO SIMPLE_ADD;

CREATE TEMPORARY MACRO flr(d bigint) FLOOR(d/10)*10;
SELECT flr(12);
DROP TEMPORARY MACRO flr;

CREATE TEMPORARY MACRO STRING_LEN(x string) length(x);
CREATE TEMPORARY MACRO STRING_LEN_PLUS_ONE(x string) length(x)+1;
CREATE TEMPORARY MACRO STRING_LEN_PLUS_TWO(x string) length(x)+2;
create table macro_test (x string) using parquet;;
insert into table macro_test values ("bb"), ("a"), ("ccc");
SELECT CONCAT(STRING_LEN(x), ":", STRING_LEN_PLUS_ONE(x), ":", STRING_LEN_PLUS_TWO(x)) a
FROM macro_test;
SELECT CONCAT(STRING_LEN(x), ":", STRING_LEN_PLUS_ONE(x), ":", STRING_LEN_PLUS_TWO(x)) a
FROM
macro_test
sort by a;
drop table macro_test;

CREATE TABLE macro_testing(a int, b int, c int) using parquet;;
insert into table macro_testing values (1,2,3);
insert into table macro_testing values (4,5,6);
CREATE TEMPORARY MACRO math_square(x int) x*x;
CREATE TEMPORARY MACRO math_add(x int) x+x;
select math_square(a), math_square(b),factorial(a), factorial(b), math_add(a), math_add(b),int(c)
from macro_testing order by int(c);
drop table macro_testing;

CREATE TEMPORARY MACRO max(x int, y int) x + y;
SELECT max(1, 2);
DROP TEMPORARY MACRO max;
SELECT max(2);

CREATE TEMPORARY MACRO c() 3E9;
SELECT floor(c()/10);
DROP TEMPORARY MACRO c;

CREATE TEMPORARY MACRO fixed_number() 42;
DROP TEMPORARY FUNCTION fixed_number;
DROP TEMPORARY MACRO IF EXISTS fixed_number;

-- invalid queries
CREATE TEMPORARY MACRO simple_add_error(x int) x + y;
CREATE TEMPORARY MACRO simple_add_error(x int, x int) x + y;
CREATE TEMPORARY MACRO simple_add_error(x int) x NOT IN (select c2);
DROP TEMPORARY MACRO SOME_MACRO;