--SET spark.sql.legacy.keepCommandOutputSchema=false
--SET spark.sql.legacy.useV1Command=false

SHOW CATALOGS;
SHOW CATALOGS 'non_existing_catalog_pattern';
SHOW CURRENT NAMESPACE;

CREATE DATABASE show_commands_db;
USE show_commands_db;
SHOW CURRENT DATABASE;

SHOW CURRENT SCHEMA;

USE default;
DROP DATABASE show_commands_db;

SHOW COLLATIONS LIKE 'utf8_*';
SHOW COLLATIONS 'UTF8_BINARY';
SHOW COLLATIONS LIKE 'non_existing_collation_pattern';

CREATE DATABASE show_listing_db;
CREATE TABLE show_listing_db.show_listing_table(id INT) USING parquet;

SHOW SCHEMAS LIKE 'show_listing*';
DECLARE show_listing_schema = 'show_listing_db';
SHOW TABLES IN IDENTIFIER(show_listing_schema) LIKE 'show_listing*';
DROP TEMPORARY VARIABLE show_listing_schema;
SHOW TABLES IN show_listing_db AS JSON;
SHOW TABLES IN show_listing_missing_db AS JSON;

CREATE FUNCTION show_listing_db.show_listing_function() RETURNS INT RETURN 1;
SHOW USER FUNCTIONS IN show_listing_db;
SHOW SYSTEM FUNCTIONS LIKE 'abs';
SHOW FUNCTIONS LIKE show_listing_db.show_listing_function;
SHOW PROCEDURES IN show_listing_db;
SHOW VIEWS IN show_listing_db.nested_namespace;

SET spark.sql.legacy.keepCommandOutputSchema=true;
SHOW TABLES IN show_listing_db;
SHOW NAMESPACES LIKE 'show_listing*';
SET spark.sql.legacy.useV1Command=true;
SHOW TABLES IN show_listing_db;
RESET spark.sql.legacy.useV1Command;
RESET spark.sql.legacy.keepCommandOutputSchema;

DROP FUNCTION show_listing_db.show_listing_function;
DROP TABLE show_listing_db.show_listing_table;
DROP DATABASE show_listing_db;
