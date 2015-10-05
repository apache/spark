-- SORT_BEFORE_DIFF

CREATE DATABASE IF NOT EXISTS test_db COMMENT 'Hive test database';
SHOW DATABASES;

GRANT drop ON DATABASE test_db TO USER hive_test_user;
GRANT select ON DATABASE test_db TO USER hive_test_user;

SHOW GRANT USER hive_test_user ON DATABASE test_db;

CREATE ROLE db_test_role;
GRANT ROLE db_test_role TO USER hive_test_user;
SHOW ROLE GRANT USER hive_test_user;

GRANT drop ON DATABASE test_db TO ROLE db_test_role;
GRANT select ON DATABASE test_db TO ROLE db_test_role;

SHOW GRANT ROLE db_test_role ON DATABASE test_db;

DROP DATABASE IF EXISTS test_db;
