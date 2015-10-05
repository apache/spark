SHOW DATABASES;

-- Try to drop a non-empty database
CREATE DATABASE test_db;
USE test_db;
CREATE TABLE t(a INT);
USE default;
DROP DATABASE test_db;
