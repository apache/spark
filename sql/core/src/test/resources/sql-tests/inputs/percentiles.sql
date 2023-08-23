-- SPARK-44871: Fix percentile_disc behaviour
SELECT
  percentile_disc(0.0) WITHIN GROUP (ORDER BY a) as p0,
  percentile_disc(0.1) WITHIN GROUP (ORDER BY a) as p1,
  percentile_disc(0.2) WITHIN GROUP (ORDER BY a) as p2,
  percentile_disc(0.3) WITHIN GROUP (ORDER BY a) as p3,
  percentile_disc(0.4) WITHIN GROUP (ORDER BY a) as p4,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY a) as p5,
  percentile_disc(0.6) WITHIN GROUP (ORDER BY a) as p6,
  percentile_disc(0.7) WITHIN GROUP (ORDER BY a) as p7,
  percentile_disc(0.8) WITHIN GROUP (ORDER BY a) as p8,
  percentile_disc(0.9) WITHIN GROUP (ORDER BY a) as p9,
  percentile_disc(1.0) WITHIN GROUP (ORDER BY a) as p10
FROM VALUES (0) AS v(a);

SELECT
  percentile_disc(0.0) WITHIN GROUP (ORDER BY a) as p0,
  percentile_disc(0.1) WITHIN GROUP (ORDER BY a) as p1,
  percentile_disc(0.2) WITHIN GROUP (ORDER BY a) as p2,
  percentile_disc(0.3) WITHIN GROUP (ORDER BY a) as p3,
  percentile_disc(0.4) WITHIN GROUP (ORDER BY a) as p4,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY a) as p5,
  percentile_disc(0.6) WITHIN GROUP (ORDER BY a) as p6,
  percentile_disc(0.7) WITHIN GROUP (ORDER BY a) as p7,
  percentile_disc(0.8) WITHIN GROUP (ORDER BY a) as p8,
  percentile_disc(0.9) WITHIN GROUP (ORDER BY a) as p9,
  percentile_disc(1.0) WITHIN GROUP (ORDER BY a) as p10
FROM VALUES (0), (1) AS v(a);

SELECT
  percentile_disc(0.0) WITHIN GROUP (ORDER BY a) as p0,
  percentile_disc(0.1) WITHIN GROUP (ORDER BY a) as p1,
  percentile_disc(0.2) WITHIN GROUP (ORDER BY a) as p2,
  percentile_disc(0.3) WITHIN GROUP (ORDER BY a) as p3,
  percentile_disc(0.4) WITHIN GROUP (ORDER BY a) as p4,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY a) as p5,
  percentile_disc(0.6) WITHIN GROUP (ORDER BY a) as p6,
  percentile_disc(0.7) WITHIN GROUP (ORDER BY a) as p7,
  percentile_disc(0.8) WITHIN GROUP (ORDER BY a) as p8,
  percentile_disc(0.9) WITHIN GROUP (ORDER BY a) as p9,
  percentile_disc(1.0) WITHIN GROUP (ORDER BY a) as p10
FROM VALUES (0), (1), (2) AS v(a);

SELECT
  percentile_disc(0.0) WITHIN GROUP (ORDER BY a) as p0,
  percentile_disc(0.1) WITHIN GROUP (ORDER BY a) as p1,
  percentile_disc(0.2) WITHIN GROUP (ORDER BY a) as p2,
  percentile_disc(0.3) WITHIN GROUP (ORDER BY a) as p3,
  percentile_disc(0.4) WITHIN GROUP (ORDER BY a) as p4,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY a) as p5,
  percentile_disc(0.6) WITHIN GROUP (ORDER BY a) as p6,
  percentile_disc(0.7) WITHIN GROUP (ORDER BY a) as p7,
  percentile_disc(0.8) WITHIN GROUP (ORDER BY a) as p8,
  percentile_disc(0.9) WITHIN GROUP (ORDER BY a) as p9,
  percentile_disc(1.0) WITHIN GROUP (ORDER BY a) as p10
FROM VALUES (0), (1), (2), (3), (4) AS v(a);

SET spark.sql.legacy.percentileDiscCalculation = true;

SELECT
  percentile_disc(0.0) WITHIN GROUP (ORDER BY a) as p0,
  percentile_disc(0.1) WITHIN GROUP (ORDER BY a) as p1,
  percentile_disc(0.2) WITHIN GROUP (ORDER BY a) as p2,
  percentile_disc(0.3) WITHIN GROUP (ORDER BY a) as p3,
  percentile_disc(0.4) WITHIN GROUP (ORDER BY a) as p4,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY a) as p5,
  percentile_disc(0.6) WITHIN GROUP (ORDER BY a) as p6,
  percentile_disc(0.7) WITHIN GROUP (ORDER BY a) as p7,
  percentile_disc(0.8) WITHIN GROUP (ORDER BY a) as p8,
  percentile_disc(0.9) WITHIN GROUP (ORDER BY a) as p9,
  percentile_disc(1.0) WITHIN GROUP (ORDER BY a) as p10
FROM VALUES (0), (1), (2), (3), (4) AS v(a);

SET spark.sql.legacy.percentileDiscCalculation = false;
