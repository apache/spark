--SET spark.sql.generator.preserveSelectListOrder=false

-- A nondeterministic expression in the first generator is evaluated after later generators.
SELECT
  explode(array(sin(0) + udf(monotonically_increasing_id()))) AS left_value,
  explode(array(10, 20)) AS right_value;

-- A nondeterministic expression in the second generator is evaluated before the first generator.
SELECT
  explode(array(udf(sin(0)), udf(1))) AS left_value,
  explode(array(monotonically_increasing_id())) AS right_value;

-- UDF, nondeterministic expression, grouping analytics, and transitive rightward generator LCA.
SELECT
  explode(array(g2)) AS g1,
  explode(array(g3)) AS g2,
  explode(array(udf(base + monotonically_increasing_id()) + sin(0))) AS g3,
  explode(array(10L, 20L)) AS independent,
  gid
FROM (
  SELECT coalesce(id, 0L) AS base, grouping_id(id) AS gid
  FROM VALUES (1L) AS t(id)
  GROUP BY GROUPING SETS ((id), ())
) grouped
WHERE gid = 1;
