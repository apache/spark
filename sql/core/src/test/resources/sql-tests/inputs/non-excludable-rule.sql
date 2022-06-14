-- SPARK-39448
SET spark.sql.optimizer.excludedRules=org.apache.spark.sql.catalyst.optimizer.ReplaceCTERefWithRepartition;
SELECT
  (SELECT min(id) FROM range(10)),
  (SELECT sum(id) FROM range(10)),
  (SELECT count(distinct id) FROM range(10));
