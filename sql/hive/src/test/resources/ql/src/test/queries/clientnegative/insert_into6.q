DROP TABLE IF EXISTS insert_into6_neg;

CREATE TABLE insert_into6_neg (key int, value string)
  PARTITIONED BY (ds string) TBLPROPERTIES("immutable"="true") ;

INSERT INTO TABLE insert_into6_neg PARTITION (ds='1')
  SELECT * FROM src LIMIT 100;

INSERT INTO TABLE insert_into6_neg PARTITION (ds='2')
  SELECT * FROM src LIMIT 100;

SELECT COUNT(*) from insert_into6_neg;

INSERT INTO TABLE insert_into6_neg PARTITION (ds='1')
  SELECT * FROM src LIMIT 100;

DROP TABLE insert_into6_neg;
