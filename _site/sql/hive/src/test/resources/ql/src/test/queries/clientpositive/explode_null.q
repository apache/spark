SELECT explode(col) AS myCol FROM 
    (SELECT array(1,2,3) AS col FROM src LIMIT 1 
     UNION ALL
     SELECT IF(false, array(1,2,3), NULL) AS col FROM src LIMIT 1) a;

SELECT explode(col) AS (myCol1,myCol2) FROM
    (SELECT map(1,'one',2,'two',3,'three') AS col FROM src LIMIT 1
     UNION ALL
     SELECT IF(false, map(1,'one',2,'two',3,'three'), NULL) AS col FROM src LIMIT 1) a;
     