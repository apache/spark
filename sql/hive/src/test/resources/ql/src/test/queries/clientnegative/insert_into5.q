DROP TABLE if exists insert_into5_neg;

CREATE TABLE insert_into5_neg (key int, value string) TBLPROPERTIES ("immutable"="true");

INSERT INTO TABLE insert_into5_neg SELECT * FROM src LIMIT 100;

INSERT INTO TABLE insert_into5_neg SELECT * FROM src LIMIT 100;

DROP TABLE insert_into5_neg;
