-- Test DISTINCT aggregates with MapType arguments.

CREATE OR REPLACE TEMPORARY VIEW distinct_map_data AS SELECT * FROM VALUES
  (2, map('a', 1, 'b', 2), 1, true),
  (2, map('b', 2, 'a', 1), 1, true),
  (1, map('a', 1, 'b', 2), 1, true),
  (1, map('a', 3), 2, false)
AS distinct_map_data(g, m, id, should_keep);

SELECT COUNT(DISTINCT m) FROM distinct_map_data;

SELECT SIZE(COLLECT_LIST(DISTINCT m)) FROM distinct_map_data;

SELECT map_entries(m)
FROM (
  SELECT EXPLODE(COLLECT_LIST(DISTINCT m)) AS m
  FROM distinct_map_data
) AS collected_maps
ORDER BY element_at(m, 'a');

SELECT map_entries(FIRST(DISTINCT m)), map_entries(LAST(DISTINCT m)), COUNT(DISTINCT m)
FROM VALUES (map('b', 2, 'a', 1)) AS single_map_data(m);

SELECT COUNT(DISTINCT m, id) FROM distinct_map_data;

SELECT COUNT(DISTINCT m), COUNT(DISTINCT id) FROM distinct_map_data;

SELECT g, COUNT(DISTINCT m)
FROM distinct_map_data
GROUP BY g
ORDER BY g;

SELECT m, COUNT(DISTINCT m), COLLECT_LIST(DISTINCT m)
FROM distinct_map_data
GROUP BY m
ORDER BY element_at(m, 'a');

SELECT COUNT(DISTINCT m) FILTER (WHERE should_keep) FROM distinct_map_data;

SELECT MAX(map_values(m)[0])
FROM distinct_map_data
WHERE id = 1;

SELECT MAX(map_values(m)[0]), COUNT(DISTINCT m)
FROM distinct_map_data
WHERE id = 1;

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

SELECT COUNT(DISTINCT array(m)) FROM distinct_map_data;

SELECT COUNT(DISTINCT map('m', m)) FROM distinct_map_data;

SELECT COUNT(DISTINCT m), COLLECT_LIST(DISTINCT m)
FROM VALUES
  (CAST(map() AS MAP<STRING, INT>)),
  (CAST(map() AS MAP<STRING, INT>)),
  (CAST(NULL AS MAP<STRING, INT>))
AS null_and_empty_map_data(m);

SELECT g, GROUPING(g), COUNT(DISTINCT m)
FROM distinct_map_data
GROUP BY GROUPING SETS ((g), ())
ORDER BY GROUPING(g), g;

SELECT COUNT(DISTINCT named_struct('m', m, 'n', n))
FROM VALUES
  (map('a', 1, 'b', 2), map('x', 1, 'y', 2)),
  (map('b', 2, 'a', 1), map('y', 2, 'x', 1))
AS grouped_distinct_map_data(m, n)
GROUP BY m;

SET spark.sql.optimizer.insertMapSortInDistinctAggregates.enabled=false;

SELECT COUNT(DISTINCT m) FROM distinct_map_data;

SELECT map_entries(m)
FROM (
  SELECT EXPLODE(COLLECT_LIST(DISTINCT m)) AS m
  FROM distinct_map_data
) AS collected_maps
ORDER BY element_at(m, 'a'), map_entries(m)[0].key;

SELECT COUNT(DISTINCT named_struct('m', m)) FROM distinct_map_data;

SELECT m, COUNT(DISTINCT m)
FROM distinct_map_data
GROUP BY m
ORDER BY element_at(m, 'a');

SET spark.sql.optimizer.insertMapSortInDistinctAggregates.enabled=true;
