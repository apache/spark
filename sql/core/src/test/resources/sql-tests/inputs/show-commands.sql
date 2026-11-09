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
