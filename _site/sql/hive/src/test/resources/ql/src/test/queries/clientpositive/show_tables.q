CREATE TABLE shtb_test1(KEY STRING, VALUE STRING) PARTITIONED BY(ds STRING) STORED AS TEXTFILE;
CREATE TABLE shtb_test2(KEY STRING, VALUE STRING) PARTITIONED BY(ds STRING) STORED AS TEXTFILE;

EXPLAIN
SHOW TABLES 'shtb_*';

SHOW TABLES 'shtb_*';

EXPLAIN
SHOW TABLES LIKE 'shtb_test1|shtb_test2';

SHOW TABLES LIKE 'shtb_test1|shtb_test2';

-- SHOW TABLES FROM/IN database
CREATE DATABASE test_db;
USE test_db;
CREATE TABLE foo(a INT);
CREATE TABLE bar(a INT);
CREATE TABLE baz(a INT);

-- SHOW TABLES basic syntax tests
USE default;
SHOW TABLES FROM test_db;
SHOW TABLES FROM default;
SHOW TABLES IN test_db;
SHOW TABLES IN default;
SHOW TABLES IN test_db "test*";
SHOW TABLES IN test_db LIKE "nomatch";

-- SHOW TABLES from a database with a name that requires escaping
CREATE DATABASE `database`;
USE `database`;
CREATE TABLE foo(a INT);
USE default;
SHOW TABLES FROM `database` LIKE "foo";
