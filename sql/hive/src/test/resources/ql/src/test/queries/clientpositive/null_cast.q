EXPLAIN SELECT ARRAY(NULL, 0), 
               ARRAY(NULL, ARRAY()),
               ARRAY(NULL, MAP()),
               ARRAY(NULL, STRUCT(0))
        FROM src tablesample (1 rows);

SELECT ARRAY(NULL, 0), 
       ARRAY(NULL, ARRAY()),
       ARRAY(NULL, MAP()),
       ARRAY(NULL, STRUCT(0))
FROM src tablesample (1 rows);
