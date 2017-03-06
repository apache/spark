CREATE OR REPLACE TEMPORARY VIEW p AS VALUES (1, 1) AS T(pk, pv);
CREATE OR REPLACE TEMPORARY VIEW c AS VALUES (1, 1) AS T(ck, cv);

-- SPARK-18814.1: Simplified version of TPCDS-Q32
SELECT pk, cv
FROM   p, c
WHERE  p.pk = c.ck
AND    c.cv = (SELECT avg(c1.cv)
               FROM   c c1
               WHERE  c1.ck = p.pk);

-- SPARK-18814.2: Adding stack of aggregates
SELECT pk, cv
FROM   p, c
WHERE  p.pk = c.ck
AND    c.cv = (SELECT max(avg)
	       FROM   (SELECT   c1.cv, avg(c1.cv) avg
		       FROM     c c1
		       WHERE    c1.ck = p.pk
                       GROUP BY c1.cv));
