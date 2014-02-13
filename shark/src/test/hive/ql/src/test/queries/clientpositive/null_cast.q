EXPLAIN SELECT ARRAY(NULL, 0), 
               ARRAY(NULL, ARRAY()),
               ARRAY(NULL, MAP()),
               ARRAY(NULL, STRUCT(0))
        FROM src LIMIT 1;

SELECT ARRAY(NULL, 0), 
       ARRAY(NULL, ARRAY()),
       ARRAY(NULL, MAP()),
       ARRAY(NULL, STRUCT(0))
FROM src LIMIT 1;
