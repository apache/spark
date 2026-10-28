--SET spark.sql.catalog.testcat=org.apache.spark.sql.connector.catalog.InMemoryCatalog

SHOW CATALOGS;
SHOW CURRENT NAMESPACE;

CREATE DATABASE show_commands_db;
USE show_commands_db;
SHOW CURRENT DATABASE;

SET CATALOG 'testcat';
SHOW CURRENT NAMESPACE;
SHOW CATALOGS;
SHOW CATALOGS LIKE 'TEST*';
SHOW CATALOGS 'non_existing_catalog_pattern';

CREATE NAMESPACE testcat.show_commands_ns.`inner.ns`;
USE testcat.show_commands_ns.`inner.ns`;
SHOW CURRENT SCHEMA;

USE spark_catalog.default;
DROP NAMESPACE testcat.show_commands_ns.`inner.ns`;
DROP DATABASE show_commands_db;

SHOW COLLATIONS LIKE 'utf8_*';
SHOW COLLATIONS 'UTF8_BINARY';
SHOW COLLATIONS LIKE 'non_existing_collation_pattern';
