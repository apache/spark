set hive.ddl.output.format=json;

CREATE TABLE IF NOT EXISTS jsontable (key INT, value STRING) COMMENT 'json table' STORED AS TEXTFILE;

SHOW TABLES;

SHOW TABLES LIKE 'json*';

SHOW TABLE EXTENDED LIKE 'json*';

ALTER TABLE jsontable SET TBLPROPERTIES ('id' = 'jsontable');

DESCRIBE jsontable;

DESCRIBE extended jsontable;

DROP TABLE jsontable;

set hive.ddl.output.format=text;
