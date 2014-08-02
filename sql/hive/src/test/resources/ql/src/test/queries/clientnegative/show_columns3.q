CREATE DATABASE test_db;
USE test_db;
CREATE TABLE foo(a INT);

use default;
SHOW COLUMNS from test_db.foo from test_db;

