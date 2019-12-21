-- This test file was converted from outer-join.sql.

-- SPARK-17099: Incorrect result when HAVING clause is added to group by query
CREATE OR REPLACE TEMPORARY VIEW t1 AS SELECT * FROM VALUES
(-234), (145), (367), (975), (298)
as t1(int_col1);

CREATE OR REPLACE TEMPORARY VIEW t2 AS SELECT * FROM VALUES
(-769, -244), (-800, -409), (940, 86), (-507, 304), (-367, 158)
as t2(int_col0, int_col1);

SELECT
  (udf(SUM(udf(COALESCE(t1.int_col1, t2.int_col0))))),
     (udf(COALESCE(t1.int_col1, t2.int_col0)) * 2)
FROM t1
RIGHT JOIN t2
  ON udf(t2.int_col0) = udf(t1.int_col1)
GROUP BY udf(GREATEST(COALESCE(udf(t2.int_col1), 109), COALESCE(t1.int_col1, udf(-449)))),
         COALESCE(t1.int_col1, t2.int_col0)
HAVING (udf(SUM(COALESCE(udf(t1.int_col1), udf(t2.int_col0)))))
            > (udf(COALESCE(t1.int_col1, t2.int_col0)) * 2);


-- SPARK-17120: Analyzer incorrectly optimizes plan to empty LocalRelation
CREATE OR REPLACE TEMPORARY VIEW t1 AS SELECT * FROM VALUES (97) as t1(int_col1);

CREATE OR REPLACE TEMPORARY VIEW t2 AS SELECT * FROM VALUES (0) as t2(int_col1);

-- Set the cross join enabled flag for the LEFT JOIN test since there's no join condition.
-- Ultimately the join should be optimized away.
set spark.sql.crossJoin.enabled = true;
SELECT *
FROM (
SELECT
    udf(COALESCE(udf(t2.int_col1), udf(t1.int_col1))) AS int_col
    FROM t1
    LEFT JOIN t2 ON false
) t where (udf(t.int_col)) is not null;
set spark.sql.crossJoin.enabled = false;


