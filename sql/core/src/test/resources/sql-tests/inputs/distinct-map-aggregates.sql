-- Test DISTINCT aggregates with MapType arguments.

CREATE OR REPLACE TEMPORARY VIEW distinct_map_data AS SELECT * FROM VALUES
  (2, map('a', 1, 'b', 2), 1, true),
  (2, map('b', 2, 'a', 1), 1, true),
  (1, map('a', 1, 'b', 2), 1, true),
  (1, map('a', 3), 2, false)
AS distinct_map_data(g, m, id, should_keep);

SET spark.sql.optimizer.insertMapSortInDistinctAggregates.enabled=true;

SELECT COUNT(DISTINCT m) FROM distinct_map_data;

SELECT SIZE(COLLECT_LIST(DISTINCT m)) FROM distinct_map_data;

SELECT COUNT(DISTINCT m, id) FROM distinct_map_data;

SELECT COUNT(DISTINCT m), COUNT(DISTINCT id) FROM distinct_map_data;

SELECT g, COUNT(DISTINCT m)
FROM distinct_map_data
GROUP BY g
ORDER BY g;

SELECT COUNT(DISTINCT m) FILTER (WHERE should_keep) FROM distinct_map_data;

SELECT g
FROM distinct_map_data
GROUP BY g
ORDER BY COUNT(DISTINCT m), g;

SELECT g
FROM distinct_map_data
GROUP BY g
HAVING COUNT(DISTINCT m) = 1
ORDER BY g;

SELECT COUNT(DISTINCT named_struct('m', m)) FROM distinct_map_data;

SET spark.sql.optimizer.insertMapSortInDistinctAggregates.enabled=false;

SELECT COUNT(DISTINCT m) FROM distinct_map_data;

SELECT SIZE(COLLECT_LIST(DISTINCT m)) FROM distinct_map_data;

SELECT COUNT(DISTINCT m, id) FROM distinct_map_data;

SELECT COUNT(DISTINCT m), COUNT(DISTINCT id) FROM distinct_map_data;

SELECT g, COUNT(DISTINCT m)
FROM distinct_map_data
GROUP BY g
ORDER BY g;

SELECT COUNT(DISTINCT m) FILTER (WHERE should_keep) FROM distinct_map_data;

SELECT g
FROM distinct_map_data
GROUP BY g
ORDER BY COUNT(DISTINCT m), g;

SELECT g
FROM distinct_map_data
GROUP BY g
HAVING COUNT(DISTINCT m) = 1
ORDER BY g;

SELECT COUNT(DISTINCT named_struct('m', m)) FROM distinct_map_data;

RESET spark.sql.optimizer.insertMapSortInDistinctAggregates.enabled;

SET spark.sql.optimizer.insertMapSortInDistinctAggregates.enabled;
