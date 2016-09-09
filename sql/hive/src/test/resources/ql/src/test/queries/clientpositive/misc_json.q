set hive.ddl.output.format=json;

CREATE TABLE IF NOT EXISTS jsontable (key INT, value STRING) COMMENT 'json table' STORED AS TEXTFILE;

ALTER TABLE jsontable ADD COLUMNS (name STRING COMMENT 'a new column'); 

ALTER TABLE jsontable RENAME TO jsontable2;

SHOW TABLE EXTENDED LIKE jsontable2;

DROP TABLE jsontable2;

set hive.ddl.output.format=text;
