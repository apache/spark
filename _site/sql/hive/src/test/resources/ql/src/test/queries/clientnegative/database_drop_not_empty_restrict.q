SHOW DATABASES;

-- Try to drop a non-empty database in restrict mode
CREATE DATABASE db_drop_non_empty_restrict;
USE db_drop_non_empty_restrict;
CREATE TABLE t(a INT);
USE default;
DROP DATABASE db_drop_non_empty_restrict;
