-- Test data.
CREATE OR REPLACE TEMPORARY VIEW aggr AS SELECT * FROM VALUES
(0, 0), (0, 10), (0, 20), (0, 30), (0, 40), (1, 10), (1, 20), (2, 10), (2, 20), (2, 25), (2, 30), (3, 60), (4, null)
AS aggr(k, v);

-- SPARK-37676: Support ANSI Aggregation Function: percentile_cont
SELECT
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v),
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC)
FROM aggr;
SELECT
  k,
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v),
  percentile_cont(0.25) WITHIN GROUP (ORDER BY v DESC)
FROM aggr
GROUP BY k
ORDER BY k;

-- SPARK-37691: Support ANSI Aggregation Function: percentile_disc
SELECT
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v),
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v DESC)
FROM aggr;
SELECT
  k,
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v),
  percentile_disc(0.25) WITHIN GROUP (ORDER BY v DESC)
FROM aggr
GROUP BY k
ORDER BY k;

-- SPARK-39320: Add the MEDIAN() function
SELECT
  median(v),
  percentile(v, 0.5),
  percentile_cont(0.5) WITHIN GROUP (ORDER BY v)
FROM aggr;
SELECT
  k,
  median(v),
  percentile(v, 0.5),
  percentile_cont(0.5) WITHIN GROUP (ORDER BY v)
FROM aggr
GROUP BY k
ORDER BY k;
