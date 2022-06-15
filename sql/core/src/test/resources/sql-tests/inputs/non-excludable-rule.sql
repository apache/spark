-- SPARK-39448
SET spark.sql.optimizer.excludedRules=org.apache.spark.sql.catalyst.optimizer.ReplaceCTERefWithRepartition;
SELECT
  (SELECT min(id) FROM range(10)),
  (SELECT sum(id) FROM range(10)),
  (SELECT count(distinct id) FROM range(10));

-- SPARK-39444
SET spark.sql.optimizer.excludedRules=org.apache.spark.sql.catalyst.optimizer.Optimizer$OptimizeSubqueries;
WITH tmp AS (
  SELECT id FROM range(2)
  INTERSECT
  SELECT id FROM range(4)
)
SELECT id FROM range(3) WHERE id > (SELECT max(id) FROM tmp);
