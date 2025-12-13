-- Test file for SHOW DATABASES / SHOW SCHEMAS (regular and AS JSON)

-- Clean up any leftover test databases first
DROP DATABASE IF EXISTS test_db1;
DROP DATABASE IF EXISTS test_db2;
DROP DATABASE IF EXISTS prod_db;
DROP DATABASE IF EXISTS dev_db;

-- Create test databases
CREATE DATABASE IF NOT EXISTS test_db1;
CREATE DATABASE IF NOT EXISTS test_db2;
CREATE DATABASE IF NOT EXISTS prod_db;
CREATE DATABASE IF NOT EXISTS dev_db;

-- Regular SHOW DATABASES
SHOW DATABASES;

-- SHOW SCHEMAS (synonym)
SHOW SCHEMAS;

-- With LIKE pattern
SHOW DATABASES LIKE 'test*';

-- With LIKE pattern (no matches)
SHOW DATABASES LIKE 'nonexistent*';

-- With IN clause
SHOW DATABASES IN spark_catalog;

-- With FROM clause (synonym)
SHOW DATABASES FROM spark_catalog;

-- Multiple patterns
SHOW DATABASES LIKE 'test_db*';
SHOW DATABASES LIKE 'prod*';
SHOW DATABASES LIKE 'dev*';

-- SHOW DATABASES AS JSON
SHOW DATABASES AS JSON;

-- SHOW SCHEMAS AS JSON (synonym)
SHOW SCHEMAS AS JSON;

-- SHOW NAMESPACES AS JSON (synonym)
SHOW NAMESPACES AS JSON;

-- With LIKE pattern
SHOW DATABASES LIKE 'test*' AS JSON;

-- With LIKE pattern (no matches)
SHOW DATABASES LIKE 'nonexistent*' AS JSON;

-- With IN clause
SHOW DATABASES IN spark_catalog AS JSON;

-- With FROM clause (synonym)
SHOW DATABASES FROM spark_catalog AS JSON;

-- Multiple patterns
SHOW DATABASES LIKE 'test_db*' AS JSON;
SHOW DATABASES LIKE 'prod*' AS JSON;
SHOW DATABASES LIKE 'dev*' AS JSON;

-- Case sensitivity test
SHOW DATABASES LIKE 'TEST*' AS JSON;
SHOW DATABASES LIKE 'Test*' AS JSON;

-- Special characters in pattern
SHOW DATABASES LIKE 'test_*' AS JSON;
SHOW DATABASES LIKE '*_db*' AS JSON;

-- Test with legacy output schema (databaseName column)
SET spark.sql.legacy.keepCommandOutputSchema=true;

SHOW DATABASES AS JSON;
SHOW DATABASES LIKE 'test*' AS JSON;

SET spark.sql.legacy.keepCommandOutputSchema=false;

-- Clean up
DROP DATABASE IF EXISTS test_db1;
DROP DATABASE IF EXISTS test_db2;
DROP DATABASE IF EXISTS prod_db;
DROP DATABASE IF EXISTS dev_db;

