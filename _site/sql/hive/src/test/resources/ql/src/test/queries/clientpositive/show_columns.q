CREATE TABLE shcol_test(KEY STRING, VALUE STRING) PARTITIONED BY(ds STRING) STORED AS TEXTFILE;

EXPLAIN
SHOW COLUMNS from shcol_test;

SHOW COLUMNS from shcol_test;

-- SHOW COLUMNS
CREATE DATABASE test_db;
USE test_db;
CREATE TABLE foo(a INT);

-- SHOW COLUMNS basic syntax tests
USE test_db;
SHOW COLUMNS from foo;
SHOW COLUMNS in foo;

-- SHOW COLUMNS from a database with a name that requires escaping
CREATE DATABASE `database`;
USE `database`;
CREATE TABLE foo(a INT);
SHOW COLUMNS from foo;

use default;
SHOW COLUMNS from test_db.foo;
SHOW COLUMNS from foo from test_db;
