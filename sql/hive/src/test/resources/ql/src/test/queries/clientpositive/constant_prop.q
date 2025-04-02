set hive.fetch.task.conversion=more;

EXPLAIN
SELECT NAMED_STRUCT(
         IF(ARRAY_CONTAINS(ARRAY(1, 2), 3), "F1", "B1"), 1,
         IF(ARRAY_CONTAINS(MAP_KEYS(MAP("b", "x")), "b"), "F2", "B2"), 2   
       ),
       NAMED_STRUCT(
         IF(ARRAY_CONTAINS(ARRAY(1, 2), 3), "F1", "B1"), 1,
         IF(ARRAY_CONTAINS(MAP_KEYS(MAP("b", "x")), "b"), "F2", "B2"), 2   
       ).F2
       FROM src tablesample (1 rows);

SELECT NAMED_STRUCT(
         IF(ARRAY_CONTAINS(ARRAY(1, 2), 3), "F1", "B1"), 1,
         IF(ARRAY_CONTAINS(MAP_KEYS(MAP("b", "x")), "b"), "F2", "B2"), 2   
       ),
       NAMED_STRUCT(
         IF(ARRAY_CONTAINS(ARRAY(1, 2), 3), "F1", "B1"), 1,
         IF(ARRAY_CONTAINS(MAP_KEYS(MAP("b", "x")), "b"), "F2", "B2"), 2   
       ).F2
       FROM src tablesample (1 rows);
