

CREATE TABLE dest_l1(key INT, value STRING) STORED AS TEXTFILE;


EXPLAIN
INSERT OVERWRITE TABLE dest_l1
SELECT j.*
FROM (SELECT t1.key, p1.value 
      FROM src1 t1 
      LEFT OUTER JOIN src p1
      ON (t1.key = p1.key)
      UNION ALL
      SELECT t2.key, p2.value
      FROM src1 t2
      LEFT OUTER JOIN src p2
      ON (t2.key = p2.key)) j;

INSERT OVERWRITE TABLE dest_l1
SELECT j.*
FROM (SELECT t1.key, p1.value
      FROM src1 t1
      LEFT OUTER JOIN src p1
      ON (t1.key = p1.key)
      UNION ALL
      SELECT t2.key, p2.value
      FROM src1 t2
      LEFT OUTER JOIN src p2
      ON (t2.key = p2.key)) j;

