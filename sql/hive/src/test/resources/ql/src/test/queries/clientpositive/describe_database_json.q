set hive.ddl.output.format=json;

CREATE DATABASE IF NOT EXISTS jsondb1 COMMENT 'Test database' LOCATION '${hiveconf:hive.metastore.warehouse.dir}/jsondb1' WITH DBPROPERTIES ('id' = 'jsondb1'); 

DESCRIBE DATABASE jsondb1;

DESCRIBE DATABASE EXTENDED jsondb1;

SHOW DATABASES;

SHOW DATABASES LIKE 'json*';

DROP DATABASE jsondb1;

CREATE DATABASE jsondb1;

DESCRIBE DATABASE jsondb1;

DESCRIBE DATABASE EXTENDED jsondb1;

DROP DATABASE jsondb1;

set hive.ddl.output.format=text;
