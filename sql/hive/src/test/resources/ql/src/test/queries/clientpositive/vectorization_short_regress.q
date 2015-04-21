SET hive.vectorized.execution.enabled=true;

-- If you look at ql/src/test/org/apache/hadoop/hive/ql/exec/vector/util/OrcFileGenerator.java
-- which is the data generation class you'll see that those values are specified in the
-- initializeFixedPointValues for each data type. When I created the queries I usedthose values
-- where I needed scalar values to ensure that when the queries executed their predicates would be
-- filtering on values that are guaranteed to exist.

-- Beyond those values, all the other data in the alltypesorc file is random, but there is a
-- specific pattern to the data that is important for coverage. In orc and subsequently
-- vectorization there are a number of optimizations for certain data patterns: AllValues, NoNulls,
-- RepeatingValue, RepeatingNull. The data in alltypesorc is generated such that each column has
-- exactly 3 batches of each data pattern. This gives us coverage for the vector expression
-- optimizations and ensure the metadata in appropriately set on the row batch object which are
-- reused across batches. 

-- For the queries themselves in order to efficiently cover as much of the new vectorization
-- functionality as I could I used a number of different techniques to create the
-- vectorization_short_regress.q test suite, primarily equivalence classes, and pairwise
-- combinations.

-- First I divided the search space into a number of dimensions such as type, aggregate function,
-- filter operation, arithmetic operation, etc. The types were explored as equivalence classes of
-- long, double, time, string, and bool. Also, rather than creating a very large number of small
-- queries the resulting vectors were grouped by compatible dimensions to reduce the number of
-- queries.

-- TargetTypeClasses: Long, Timestamp, Double, String, Bool
-- Functions: Avg, Sum, StDevP, StDev, Var, Min, Count
-- ArithmeticOps: Add, Multiply, Subtract, Divide
-- FilterOps: Equal, NotEqual, GreaterThan, LessThan, LessThanOrEqual
-- GroupBy: NoGroupByProjectAggs
EXPLAIN SELECT AVG(cint),
       (AVG(cint) + -3728),
       (-((AVG(cint) + -3728))),
       (-((-((AVG(cint) + -3728))))),
       ((-((-((AVG(cint) + -3728))))) * (AVG(cint) + -3728)),
       SUM(cdouble),
       (-(AVG(cint))),
       STDDEV_POP(cint),
       (((-((-((AVG(cint) + -3728))))) * (AVG(cint) + -3728)) * (-((-((AVG(cint) + -3728)))))),
       STDDEV_SAMP(csmallint),
       (-(STDDEV_POP(cint))),
       (STDDEV_POP(cint) - (-((-((AVG(cint) + -3728)))))),
       ((STDDEV_POP(cint) - (-((-((AVG(cint) + -3728)))))) * STDDEV_POP(cint)),
       VAR_SAMP(cint),
       AVG(cfloat),
       (10.175 - VAR_SAMP(cint)),
       (-((10.175 - VAR_SAMP(cint)))),
       ((-(STDDEV_POP(cint))) / -563),
       STDDEV_SAMP(cint),
       (-(((-(STDDEV_POP(cint))) / -563))),
       (AVG(cint) / SUM(cdouble)),
       MIN(ctinyint),
       COUNT(csmallint),
       (MIN(ctinyint) / ((-(STDDEV_POP(cint))) / -563)),
       (-((AVG(cint) / SUM(cdouble))))
FROM   alltypesorc
WHERE  ((762 = cbigint)
        OR ((csmallint < cfloat)
            AND ((ctimestamp2 > -10669)
                 AND (cdouble != cint)))
        OR (cstring1 = 'a')
           OR ((cbigint <= -1.389)
               AND ((cstring2 != 'a')
                    AND ((79.553 != cint)
                         AND (cboolean2 != cboolean1)))));
SELECT AVG(cint),
       (AVG(cint) + -3728),
       (-((AVG(cint) + -3728))),
       (-((-((AVG(cint) + -3728))))),
       ((-((-((AVG(cint) + -3728))))) * (AVG(cint) + -3728)),
       SUM(cdouble),
       (-(AVG(cint))),
       STDDEV_POP(cint),
       (((-((-((AVG(cint) + -3728))))) * (AVG(cint) + -3728)) * (-((-((AVG(cint) + -3728)))))),
       STDDEV_SAMP(csmallint),
       (-(STDDEV_POP(cint))),
       (STDDEV_POP(cint) - (-((-((AVG(cint) + -3728)))))),
       ((STDDEV_POP(cint) - (-((-((AVG(cint) + -3728)))))) * STDDEV_POP(cint)),
       VAR_SAMP(cint),
       AVG(cfloat),
       (10.175 - VAR_SAMP(cint)),
       (-((10.175 - VAR_SAMP(cint)))),
       ((-(STDDEV_POP(cint))) / -563),
       STDDEV_SAMP(cint),
       (-(((-(STDDEV_POP(cint))) / -563))),
       (AVG(cint) / SUM(cdouble)),
       MIN(ctinyint),
       COUNT(csmallint),
       (MIN(ctinyint) / ((-(STDDEV_POP(cint))) / -563)),
       (-((AVG(cint) / SUM(cdouble))))
FROM   alltypesorc
WHERE  ((762 = cbigint)
        OR ((csmallint < cfloat)
            AND ((ctimestamp2 > -10669)
                 AND (cdouble != cint)))
        OR (cstring1 = 'a')
           OR ((cbigint <= -1.389)
               AND ((cstring2 != 'a')
                    AND ((79.553 != cint)
                         AND (cboolean2 != cboolean1)))));

-- TargetTypeClasses: Long, Bool, Double, String, Timestamp
-- Functions: Max, VarP, StDevP, Avg, Min, StDev, Var
-- ArithmeticOps: Divide, Multiply, Remainder, Subtract
-- FilterOps: LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual, Like, RLike
-- GroupBy: NoGroupByProjectAggs
EXPLAIN SELECT MAX(cint),
       (MAX(cint) / -3728),
       (MAX(cint) * -3728),
       VAR_POP(cbigint),
       (-((MAX(cint) * -3728))),
       STDDEV_POP(csmallint),
       (-563 % (MAX(cint) * -3728)),
       (VAR_POP(cbigint) / STDDEV_POP(csmallint)),
       (-(STDDEV_POP(csmallint))),
       MAX(cdouble),
       AVG(ctinyint),
       (STDDEV_POP(csmallint) - 10.175),
       MIN(cint),
       ((MAX(cint) * -3728) % (STDDEV_POP(csmallint) - 10.175)),
       (-(MAX(cdouble))),
       MIN(cdouble),
       (MAX(cdouble) % -26.28),
       STDDEV_SAMP(csmallint),
       (-((MAX(cint) / -3728))),
       ((-((MAX(cint) * -3728))) % (-563 % (MAX(cint) * -3728))),
       ((MAX(cint) / -3728) - AVG(ctinyint)),
       (-((MAX(cint) * -3728))),
       VAR_SAMP(cint)
FROM   alltypesorc
WHERE  (((cbigint <= 197)
         AND (cint < cbigint))
        OR ((cdouble >= -26.28)
            AND (csmallint > cdouble))
        OR ((ctinyint > cfloat)
            AND (cstring1 RLIKE '.*ss.*'))
           OR ((cfloat > 79.553)
               AND (cstring2 LIKE '10%')));
SELECT MAX(cint),
       (MAX(cint) / -3728),
       (MAX(cint) * -3728),
       VAR_POP(cbigint),
       (-((MAX(cint) * -3728))),
       STDDEV_POP(csmallint),
       (-563 % (MAX(cint) * -3728)),
       (VAR_POP(cbigint) / STDDEV_POP(csmallint)),
       (-(STDDEV_POP(csmallint))),
       MAX(cdouble),
       AVG(ctinyint),
       (STDDEV_POP(csmallint) - 10.175),
       MIN(cint),
       ((MAX(cint) * -3728) % (STDDEV_POP(csmallint) - 10.175)),
       (-(MAX(cdouble))),
       MIN(cdouble),
       (MAX(cdouble) % -26.28),
       STDDEV_SAMP(csmallint),
       (-((MAX(cint) / -3728))),
       ((-((MAX(cint) * -3728))) % (-563 % (MAX(cint) * -3728))),
       ((MAX(cint) / -3728) - AVG(ctinyint)),
       (-((MAX(cint) * -3728))),
       VAR_SAMP(cint)
FROM   alltypesorc
WHERE  (((cbigint <= 197)
         AND (cint < cbigint))
        OR ((cdouble >= -26.28)
            AND (csmallint > cdouble))
        OR ((ctinyint > cfloat)
            AND (cstring1 RLIKE '.*ss.*'))
           OR ((cfloat > 79.553)
               AND (cstring2 LIKE '10%')));

-- TargetTypeClasses: String, Long, Bool, Double, Timestamp
-- Functions: VarP, Count, Max, StDevP, StDev, Avg
-- ArithmeticOps: Subtract, Remainder, Multiply, Add
-- FilterOps: Equal, LessThanOrEqual, GreaterThan, Like, LessThan
-- GroupBy: NoGroupByProjectAggs
EXPLAIN SELECT VAR_POP(cbigint),
       (-(VAR_POP(cbigint))),
       (VAR_POP(cbigint) - (-(VAR_POP(cbigint)))),
       COUNT(*),
       (COUNT(*) % 79.553),
       MAX(ctinyint),
       (COUNT(*) - (-(VAR_POP(cbigint)))),
       (-((-(VAR_POP(cbigint))))),
       (-1 % (-(VAR_POP(cbigint)))),
       COUNT(*),
       (-(COUNT(*))),
       STDDEV_POP(csmallint),
       (-((-((-(VAR_POP(cbigint))))))),
       (762 * (-(COUNT(*)))),
       MAX(cint),
       (MAX(ctinyint) + (762 * (-(COUNT(*))))),
       ((-(VAR_POP(cbigint))) + MAX(cint)),
       STDDEV_SAMP(cdouble),
       ((-(COUNT(*))) % COUNT(*)),
       COUNT(ctinyint),
       AVG(ctinyint),
       (-3728 % (MAX(ctinyint) + (762 * (-(COUNT(*))))))
FROM   alltypesorc
WHERE  ((ctimestamp1 = ctimestamp2)
        OR (762 = cfloat)
        OR (cstring1 = 'ss')
           OR ((csmallint <= cbigint)
               AND (1 = cboolean2))
              OR ((cboolean1 IS NOT NULL)
                  AND ((ctimestamp2 IS NOT NULL)
                       AND (cstring2 > 'a'))));
SELECT VAR_POP(cbigint),
       (-(VAR_POP(cbigint))),
       (VAR_POP(cbigint) - (-(VAR_POP(cbigint)))),
       COUNT(*),
       (COUNT(*) % 79.553),
       MAX(ctinyint),
       (COUNT(*) - (-(VAR_POP(cbigint)))),
       (-((-(VAR_POP(cbigint))))),
       (-1 % (-(VAR_POP(cbigint)))),
       COUNT(*),
       (-(COUNT(*))),
       STDDEV_POP(csmallint),
       (-((-((-(VAR_POP(cbigint))))))),
       (762 * (-(COUNT(*)))),
       MAX(cint),
       (MAX(ctinyint) + (762 * (-(COUNT(*))))),
       ((-(VAR_POP(cbigint))) + MAX(cint)),
       STDDEV_SAMP(cdouble),
       ((-(COUNT(*))) % COUNT(*)),
       COUNT(ctinyint),
       AVG(ctinyint),
       (-3728 % (MAX(ctinyint) + (762 * (-(COUNT(*))))))
FROM   alltypesorc
WHERE  ((ctimestamp1 = ctimestamp2)
        OR (762 = cfloat)
        OR (cstring1 = 'ss')
           OR ((csmallint <= cbigint)
               AND (1 = cboolean2))
              OR ((cboolean1 IS NOT NULL)
                  AND ((ctimestamp2 IS NOT NULL)
                       AND (cstring2 > 'a'))));

-- TargetTypeClasses: String, Bool, Timestamp, Long, Double
-- Functions: Avg, Max, StDev, VarP
-- ArithmeticOps: Add, Divide, Remainder, Multiply
-- FilterOps: LessThanOrEqual, NotEqual, GreaterThanOrEqual, LessThan, Equal
-- GroupBy: NoGroupByProjectAggs
EXPLAIN SELECT AVG(ctinyint),
       (AVG(ctinyint) + 6981),
       ((AVG(ctinyint) + 6981) + AVG(ctinyint)),
       MAX(cbigint),
       (((AVG(ctinyint) + 6981) + AVG(ctinyint)) / AVG(ctinyint)),
       (-((AVG(ctinyint) + 6981))),
       STDDEV_SAMP(cint),
       (AVG(ctinyint) % (-((AVG(ctinyint) + 6981)))),
       VAR_POP(cint),
       VAR_POP(cbigint),
       (-(MAX(cbigint))),
       ((-(MAX(cbigint))) / STDDEV_SAMP(cint)),
       MAX(cfloat),
       (VAR_POP(cbigint) * -26.28)
FROM   alltypesorc
WHERE  (((ctimestamp2 <= ctimestamp1)
         AND ((cbigint != cdouble)
              AND ('ss' <= cstring1)))
        OR ((csmallint < ctinyint)
            AND (ctimestamp1 >= 0))
           OR (cfloat = 17));
SELECT AVG(ctinyint),
       (AVG(ctinyint) + 6981),
       ((AVG(ctinyint) + 6981) + AVG(ctinyint)),
       MAX(cbigint),
       (((AVG(ctinyint) + 6981) + AVG(ctinyint)) / AVG(ctinyint)),
       (-((AVG(ctinyint) + 6981))),
       STDDEV_SAMP(cint),
       (AVG(ctinyint) % (-((AVG(ctinyint) + 6981)))),
       VAR_POP(cint),
       VAR_POP(cbigint),
       (-(MAX(cbigint))),
       ((-(MAX(cbigint))) / STDDEV_SAMP(cint)),
       MAX(cfloat),
       (VAR_POP(cbigint) * -26.28)
FROM   alltypesorc
WHERE  (((ctimestamp2 <= ctimestamp1)
         AND ((cbigint != cdouble)
              AND ('ss' <= cstring1)))
        OR ((csmallint < ctinyint)
            AND (ctimestamp1 >= 0))
           OR (cfloat = 17));

-- TargetTypeClasses: Timestamp, String, Long, Double, Bool
-- Functions: Max, Avg, Min, Var, StDev, Count, StDevP, Sum
-- ArithmeticOps: Multiply, Subtract, Add, Divide
-- FilterOps: Like, NotEqual, LessThan, GreaterThanOrEqual, GreaterThan, RLike
-- GroupBy: NoGroupByProjectColumns
EXPLAIN SELECT cint,
       cdouble,
       ctimestamp2,
       cstring1,
       cboolean2,
       ctinyint,
       cfloat,
       ctimestamp1,
       csmallint,
       cbigint,
       (-3728 * cbigint),
       (-(cint)),
       (-863.257 - cint),
       (-(csmallint)),
       (csmallint - (-(csmallint))),
       ((csmallint - (-(csmallint))) + (-(csmallint))),
       (cint / cint),
       ((-863.257 - cint) - -26.28),
       (-(cfloat)),
       (cdouble * -89010),
       (ctinyint / 988888),
       (-(ctinyint)),
       (79.553 / ctinyint)
FROM   alltypesorc
WHERE  (((cstring1 RLIKE 'a.*')
         AND (cstring2 LIKE '%ss%'))
        OR ((1 != cboolean2)
            AND ((csmallint < 79.553)
                 AND (-257 != ctinyint)))
        OR ((cdouble > ctinyint)
            AND (cfloat >= cint))
           OR ((cint < cbigint)
               AND (ctinyint > cbigint)));
SELECT cint,
       cdouble,
       ctimestamp2,
       cstring1,
       cboolean2,
       ctinyint,
       cfloat,
       ctimestamp1,
       csmallint,
       cbigint,
       (-3728 * cbigint),
       (-(cint)),
       (-863.257 - cint),
       (-(csmallint)),
       (csmallint - (-(csmallint))),
       ((csmallint - (-(csmallint))) + (-(csmallint))),
       (cint / cint),
       ((-863.257 - cint) - -26.28),
       (-(cfloat)),
       (cdouble * -89010),
       (ctinyint / 988888),
       (-(ctinyint)),
       (79.553 / ctinyint)
FROM   alltypesorc
WHERE  (((cstring1 RLIKE 'a.*')
         AND (cstring2 LIKE '%ss%'))
        OR ((1 != cboolean2)
            AND ((csmallint < 79.553)
                 AND (-257 != ctinyint)))
        OR ((cdouble > ctinyint)
            AND (cfloat >= cint))
           OR ((cint < cbigint)
               AND (ctinyint > cbigint)));

-- TargetTypeClasses: Long, String, Double, Bool, Timestamp
-- Functions: VarP, Var, StDev, StDevP, Max, Sum
-- ArithmeticOps: Divide, Remainder, Subtract, Multiply
-- FilterOps: Equal, LessThanOrEqual, LessThan, Like, GreaterThanOrEqual, NotEqual, GreaterThan
-- GroupBy: NoGroupByProjectColumns
EXPLAIN SELECT cint,
       cbigint,
       cstring1,
       cboolean1,
       cfloat,
       cdouble,
       ctimestamp2,
       csmallint,
       cstring2,
       cboolean2,
       (cint / cbigint),
       (cbigint % 79.553),
       (-((cint / cbigint))),
       (10.175 % cfloat),
       (-(cfloat)),
       (cfloat - (-(cfloat))),
       ((cfloat - (-(cfloat))) % -6432),
       (cdouble * csmallint),
       (-(cdouble)),
       (-(cbigint)),
       (cfloat - (cint / cbigint)),
       (-(csmallint)),
       (3569 % cbigint),
       (359 - cdouble),
       (-(csmallint))
FROM   alltypesorc
WHERE  (((197 > ctinyint)
         AND (cint = cbigint))
        OR (cbigint = 359)
        OR (cboolean1 < 0)
           OR ((cstring1 LIKE '%ss')
               AND (cfloat <= ctinyint)));

SELECT cint,
       cbigint,
       cstring1,
       cboolean1,
       cfloat,
       cdouble,
       ctimestamp2,
       csmallint,
       cstring2,
       cboolean2,
       (cint / cbigint),
       (cbigint % 79.553),
       (-((cint / cbigint))),
       (10.175 % cfloat),
       (-(cfloat)),
       (cfloat - (-(cfloat))),
       ((cfloat - (-(cfloat))) % -6432),
       (cdouble * csmallint),
       (-(cdouble)),
       (-(cbigint)),
       (cfloat - (cint / cbigint)),
       (-(csmallint)),
       (3569 % cbigint),
       (359 - cdouble),
       (-(csmallint))
FROM   alltypesorc
WHERE  (((197 > ctinyint)
         AND (cint = cbigint))
        OR (cbigint = 359)
        OR (cboolean1 < 0)
           OR ((cstring1 LIKE '%ss')
               AND (cfloat <= ctinyint)));

-- TargetTypeClasses: String, Bool, Double, Long, Timestamp
-- Functions: Sum, Max, Avg, Var, StDevP, VarP
-- ArithmeticOps: Add, Subtract, Divide, Multiply, Remainder
-- FilterOps: NotEqual, GreaterThanOrEqual, Like, LessThanOrEqual, Equal, GreaterThan
-- GroupBy: NoGroupByProjectColumns
EXPLAIN SELECT   cint,
         cstring1,
         cboolean2,
         ctimestamp2,
         cdouble,
         cfloat,
         cbigint,
         csmallint,
         cboolean1,
         (cint + csmallint),
         (cbigint - ctinyint),
         (-(cbigint)),
         (-(cfloat)),
         ((cbigint - ctinyint) + cbigint),
         (cdouble / cdouble),
         (-(cdouble)),
         ((cint + csmallint) * (-(cbigint))),
         ((-(cdouble)) + cbigint),
         (-1.389 / ctinyint),
         (cbigint % cdouble),
         (-(csmallint)),
         (csmallint + (cint + csmallint))
FROM     alltypesorc
WHERE    (((csmallint > -26.28)
           AND (cstring2 LIKE 'ss'))
          OR ((cdouble <= cbigint)
              AND ((cstring1 >= 'ss')
                   AND (cint != cdouble)))
          OR (ctinyint = -89010)
             OR ((cbigint <= cfloat)
                 AND (-26.28 <= csmallint)))
ORDER BY cboolean1, cstring1, ctimestamp2, cfloat, cbigint, cstring1, cdouble, cint, csmallint, cdouble;
SELECT   cint,
         cstring1,
         cboolean2,
         ctimestamp2,
         cdouble,
         cfloat,
         cbigint,
         csmallint,
         cboolean1,
         (cint + csmallint),
         (cbigint - ctinyint),
         (-(cbigint)),
         (-(cfloat)),
         ((cbigint - ctinyint) + cbigint),
         (cdouble / cdouble),
         (-(cdouble)),
         ((cint + csmallint) * (-(cbigint))),
         ((-(cdouble)) + cbigint),
         (-1.389 / ctinyint),
         (cbigint % cdouble),
         (-(csmallint)),
         (csmallint + (cint + csmallint))
FROM     alltypesorc
WHERE    (((csmallint > -26.28)
           AND (cstring2 LIKE 'ss'))
          OR ((cdouble <= cbigint)
              AND ((cstring1 >= 'ss')
                   AND (cint != cdouble)))
          OR (ctinyint = -89010)
             OR ((cbigint <= cfloat)
                 AND (-26.28 <= csmallint)))
ORDER BY cboolean1, cstring1, ctimestamp2, cfloat, cbigint, cstring1, cdouble, cint, csmallint, cdouble;

-- TargetTypeClasses: Long, String, Double, Timestamp
-- Functions: Avg, Min, StDevP, Sum, Var
-- ArithmeticOps: Divide, Subtract, Multiply, Remainder
-- FilterOps: GreaterThan, LessThan, LessThanOrEqual, GreaterThanOrEqual, Like
-- GroupBy: NoGroupByProjectColumns
EXPLAIN SELECT   ctimestamp1,
         cstring2,
         cdouble,
         cfloat,
         cbigint,
         csmallint,
         (cbigint / 3569),
         (-257 - csmallint),
         (-6432 * cfloat),
         (-(cdouble)),
         (cdouble * 10.175),
         ((-6432 * cfloat) / cfloat),
         (-(cfloat)),
         (cint % csmallint),
         (-(cdouble)),
         (cdouble * (-(cdouble)))
FROM     alltypesorc
WHERE    (((-1.389 >= cint)
           AND ((csmallint < ctinyint)
                AND (-6432 > csmallint)))
          OR ((cdouble >= cfloat)
              AND (cstring2 <= 'a'))
             OR ((cstring1 LIKE 'ss%')
                 AND (10.175 > cbigint)))
ORDER BY csmallint, cstring2, cdouble;
SELECT   ctimestamp1,
         cstring2,
         cdouble,
         cfloat,
         cbigint,
         csmallint,
         (cbigint / 3569),
         (-257 - csmallint),
         (-6432 * cfloat),
         (-(cdouble)),
         (cdouble * 10.175),
         ((-6432 * cfloat) / cfloat),
         (-(cfloat)),
         (cint % csmallint),
         (-(cdouble)),
         (cdouble * (-(cdouble)))
FROM     alltypesorc
WHERE    (((-1.389 >= cint)
           AND ((csmallint < ctinyint)
                AND (-6432 > csmallint)))
          OR ((cdouble >= cfloat)
              AND (cstring2 <= 'a'))
             OR ((cstring1 LIKE 'ss%')
                 AND (10.175 > cbigint)))
ORDER BY csmallint, cstring2, cdouble;

-- TargetTypeClasses: Double, String, Long
-- Functions: StDev, Sum, VarP, Count
-- ArithmeticOps: Remainder, Divide, Subtract
-- FilterOps: GreaterThanOrEqual, Equal, LessThanOrEqual
-- GroupBy: GroupBy
EXPLAIN SELECT   csmallint,
         (csmallint % -75),
         STDDEV_SAMP(csmallint),
         (-1.389 / csmallint),
         SUM(cbigint),
         ((csmallint % -75) / SUM(cbigint)),
         (-((csmallint % -75))),
         VAR_POP(ctinyint),
         (-((-((csmallint % -75))))),
         COUNT(*),
         (COUNT(*) - -89010)
FROM     alltypesorc
WHERE    (((csmallint >= -257))
          AND ((-6432 = csmallint)
               OR ((cint >= cdouble)
                   AND (ctinyint <= cint))))
GROUP BY csmallint
ORDER BY csmallint;
SELECT   csmallint,
         (csmallint % -75),
         STDDEV_SAMP(csmallint),
         (-1.389 / csmallint),
         SUM(cbigint),
         ((csmallint % -75) / SUM(cbigint)),
         (-((csmallint % -75))),
         VAR_POP(ctinyint),
         (-((-((csmallint % -75))))),
         COUNT(*),
         (COUNT(*) - -89010)
FROM     alltypesorc
WHERE    (((csmallint >= -257))
          AND ((-6432 = csmallint)
               OR ((cint >= cdouble)
                   AND (ctinyint <= cint))))
GROUP BY csmallint
ORDER BY csmallint;

-- TargetTypeClasses: Long, Double, Timestamp
-- Functions: Var, Count, Sum, VarP, StDevP
-- ArithmeticOps: Multiply, Add, Subtract, Remainder
-- FilterOps: GreaterThan, LessThan, Equal, LessThanOrEqual, GreaterThanOrEqual
-- GroupBy: GroupBy
EXPLAIN SELECT   cdouble,
         VAR_SAMP(cdouble),
         (2563.58 * VAR_SAMP(cdouble)),
         (-(VAR_SAMP(cdouble))),
         COUNT(cfloat),
         ((2563.58 * VAR_SAMP(cdouble)) + -5638.15),
         ((-(VAR_SAMP(cdouble))) * ((2563.58 * VAR_SAMP(cdouble)) + -5638.15)),
         SUM(cfloat),
         VAR_POP(cdouble),
         (cdouble - (-(VAR_SAMP(cdouble)))),
         STDDEV_POP(cdouble),
         (cdouble + VAR_SAMP(cdouble)),
         (cdouble * 762),
         SUM(cdouble),
         (-863.257 % (cdouble * 762)),
         SUM(cdouble)
FROM     alltypesorc
WHERE    (((cdouble > 2563.58))
          AND (((cbigint >= cint)
                AND ((csmallint < cint)
                     AND (cfloat < -5638.15)))
               OR (2563.58 = ctinyint)
                  OR ((cdouble <= cbigint)
                      AND (-5638.15 > cbigint))))
GROUP BY cdouble
ORDER BY cdouble;
SELECT   cdouble,
         VAR_SAMP(cdouble),
         (2563.58 * VAR_SAMP(cdouble)),
         (-(VAR_SAMP(cdouble))),
         COUNT(cfloat),
         ((2563.58 * VAR_SAMP(cdouble)) + -5638.15),
         ((-(VAR_SAMP(cdouble))) * ((2563.58 * VAR_SAMP(cdouble)) + -5638.15)),
         SUM(cfloat),
         VAR_POP(cdouble),
         (cdouble - (-(VAR_SAMP(cdouble)))),
         STDDEV_POP(cdouble),
         (cdouble + VAR_SAMP(cdouble)),
         (cdouble * 762),
         SUM(cdouble),
         (-863.257 % (cdouble * 762)),
         SUM(cdouble)
FROM     alltypesorc
WHERE    (((cdouble > 2563.58))
          AND (((cbigint >= cint)
                AND ((csmallint < cint)
                     AND (cfloat < -5638.15)))
               OR (2563.58 = ctinyint)
                  OR ((cdouble <= cbigint)
                      AND (-5638.15 > cbigint))))
GROUP BY cdouble
ORDER BY cdouble;

-- TargetTypeClasses: Bool, Timestamp, String, Double, Long
-- Functions: StDevP, Avg, Count, Min, Var, VarP, Sum
-- ArithmeticOps: Multiply, Subtract, Add, Divide, Remainder
-- FilterOps: NotEqual, LessThan, Like, Equal, RLike
-- GroupBy: GroupBy
EXPLAIN SELECT   ctimestamp1,
         cstring1,
         STDDEV_POP(cint),
         (STDDEV_POP(cint) * 10.175),
         (-(STDDEV_POP(cint))),
         AVG(csmallint),
         (-(STDDEV_POP(cint))),
         (-26.28 - STDDEV_POP(cint)),
         COUNT(*),
         (-(COUNT(*))),
         ((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))),
         MIN(ctinyint),
         (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*)))),
         (-((STDDEV_POP(cint) * 10.175))),
         VAR_SAMP(csmallint),
         (VAR_SAMP(csmallint) + (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*))))),
         (-((-(STDDEV_POP(cint))))),
         ((-(COUNT(*))) / STDDEV_POP(cint)),
         VAR_POP(cfloat),
         (10.175 / AVG(csmallint)),
         AVG(cint),
         VAR_SAMP(cfloat),
         ((VAR_SAMP(csmallint) + (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*))))) - (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*))))),
         (-((-((STDDEV_POP(cint) * 10.175))))),
         AVG(cfloat),
         (((VAR_SAMP(csmallint) + (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*))))) - (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*))))) * 10.175),
         (10.175 % (10.175 / AVG(csmallint))),
         (-(MIN(ctinyint))),
         MIN(cdouble),
         VAR_POP(csmallint),
         (-(((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))))),
         ((-(STDDEV_POP(cint))) % AVG(cfloat)),
         (-26.28 / (-(MIN(ctinyint)))),
         STDDEV_POP(ctinyint),
         SUM(cint),
         ((VAR_SAMP(csmallint) + (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*))))) / VAR_POP(cfloat)),
         (-((-(COUNT(*))))),
         COUNT(*),
         ((VAR_SAMP(csmallint) + (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*))))) % -26.28)
FROM     alltypesorc
WHERE    (((ctimestamp1 != 0))
          AND ((((-257 != ctinyint)
                 AND (cboolean2 IS NOT NULL))
                AND ((cstring1 RLIKE '.*ss')
                     AND (-10669 < ctimestamp1)))
               OR (ctimestamp2 = -10669)
               OR ((ctimestamp1 < 0)
                   AND (cstring2 LIKE '%b%'))
                  OR (cdouble = cint)
                     OR ((cboolean1 IS NULL)
                         AND (cfloat < cint))))
GROUP BY ctimestamp1, cstring1;
SELECT   ctimestamp1,
         cstring1,
         STDDEV_POP(cint),
         (STDDEV_POP(cint) * 10.175),
         (-(STDDEV_POP(cint))),
         AVG(csmallint),
         (-(STDDEV_POP(cint))),
         (-26.28 - STDDEV_POP(cint)),
         COUNT(*),
         (-(COUNT(*))),
         ((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))),
         MIN(ctinyint),
         (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*)))),
         (-((STDDEV_POP(cint) * 10.175))),
         VAR_SAMP(csmallint),
         (VAR_SAMP(csmallint) + (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*))))),
         (-((-(STDDEV_POP(cint))))),
         ((-(COUNT(*))) / STDDEV_POP(cint)),
         VAR_POP(cfloat),
         (10.175 / AVG(csmallint)),
         AVG(cint),
         VAR_SAMP(cfloat),
         ((VAR_SAMP(csmallint) + (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*))))) - (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*))))),
         (-((-((STDDEV_POP(cint) * 10.175))))),
         AVG(cfloat),
         (((VAR_SAMP(csmallint) + (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*))))) - (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*))))) * 10.175),
         (10.175 % (10.175 / AVG(csmallint))),
         (-(MIN(ctinyint))),
         MIN(cdouble),
         VAR_POP(csmallint),
         (-(((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))))),
         ((-(STDDEV_POP(cint))) % AVG(cfloat)),
         (-26.28 / (-(MIN(ctinyint)))),
         STDDEV_POP(ctinyint),
         SUM(cint),
         ((VAR_SAMP(csmallint) + (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*))))) / VAR_POP(cfloat)),
         (-((-(COUNT(*))))),
         COUNT(*),
         ((VAR_SAMP(csmallint) + (((-26.28 - STDDEV_POP(cint)) * (-(STDDEV_POP(cint)))) * (-(COUNT(*))))) % -26.28)
FROM     alltypesorc
WHERE    (((ctimestamp1 != 0))
          AND ((((-257 != ctinyint)
                 AND (cboolean2 IS NOT NULL))
                AND ((cstring1 RLIKE '.*ss')
                     AND (-10669 < ctimestamp1)))
               OR (ctimestamp2 = -10669)
               OR ((ctimestamp1 < 0)
                   AND (cstring2 LIKE '%b%'))
                  OR (cdouble = cint)
                     OR ((cboolean1 IS NULL)
                         AND (cfloat < cint))))
GROUP BY ctimestamp1, cstring1;

-- TargetTypeClasses: Double, Long, String, Timestamp, Bool
-- Functions: Max, Sum, Var, Avg, Min, VarP, StDev, StDevP
-- ArithmeticOps: Divide, Subtract, Remainder, Add, Multiply
-- FilterOps: GreaterThan, LessThanOrEqual, Equal, LessThan, GreaterThanOrEqual, NotEqual, Like, RLike
-- GroupBy: GroupBy
EXPLAIN SELECT   cboolean1,
         MAX(cfloat),
         (-(MAX(cfloat))),
         (-26.28 / MAX(cfloat)),
         SUM(cbigint),
         (SUM(cbigint) - 10.175),
         VAR_SAMP(cint),
         (VAR_SAMP(cint) % MAX(cfloat)),
         (10.175 + (-(MAX(cfloat)))),
         AVG(cdouble),
         ((SUM(cbigint) - 10.175) + VAR_SAMP(cint)),
         MIN(cbigint),
         VAR_POP(cbigint),
         (-((10.175 + (-(MAX(cfloat)))))),
         (79.553 / VAR_POP(cbigint)),
         (VAR_SAMP(cint) % (79.553 / VAR_POP(cbigint))),
         (-((10.175 + (-(MAX(cfloat)))))),
         SUM(cint),
         STDDEV_SAMP(ctinyint),
         (-1.389 * MIN(cbigint)),
         (SUM(cint) - (-1.389 * MIN(cbigint))),
         STDDEV_POP(csmallint),
         (-((SUM(cint) - (-1.389 * MIN(cbigint))))),
         AVG(cint),
         (-(AVG(cint))),
         (AVG(cint) * SUM(cint))
FROM     alltypesorc
WHERE    (((cboolean1 IS NOT NULL))
          AND (((cdouble < csmallint)
                AND ((cboolean2 = cboolean1)
                     AND (cbigint <= -863.257)))
               OR ((cint >= -257)
                   AND ((cstring1 IS NOT NULL)
                        AND (cboolean1 >= 1)))
               OR (cstring2 RLIKE 'b')
                  OR ((csmallint >= ctinyint)
                      AND (ctimestamp2 IS NULL))))
GROUP BY cboolean1
ORDER BY cboolean1;
SELECT   cboolean1,
         MAX(cfloat),
         (-(MAX(cfloat))),
         (-26.28 / MAX(cfloat)),
         SUM(cbigint),
         (SUM(cbigint) - 10.175),
         VAR_SAMP(cint),
         (VAR_SAMP(cint) % MAX(cfloat)),
         (10.175 + (-(MAX(cfloat)))),
         AVG(cdouble),
         ((SUM(cbigint) - 10.175) + VAR_SAMP(cint)),
         MIN(cbigint),
         VAR_POP(cbigint),
         (-((10.175 + (-(MAX(cfloat)))))),
         (79.553 / VAR_POP(cbigint)),
         (VAR_SAMP(cint) % (79.553 / VAR_POP(cbigint))),
         (-((10.175 + (-(MAX(cfloat)))))),
         SUM(cint),
         STDDEV_SAMP(ctinyint),
         (-1.389 * MIN(cbigint)),
         (SUM(cint) - (-1.389 * MIN(cbigint))),
         STDDEV_POP(csmallint),
         (-((SUM(cint) - (-1.389 * MIN(cbigint))))),
         AVG(cint),
         (-(AVG(cint))),
         (AVG(cint) * SUM(cint))
FROM     alltypesorc
WHERE    (((cboolean1 IS NOT NULL))
          AND (((cdouble < csmallint)
                AND ((cboolean2 = cboolean1)
                     AND (cbigint <= -863.257)))
               OR ((cint >= -257)
                   AND ((cstring1 IS NOT NULL)
                        AND (cboolean1 >= 1)))
               OR (cstring2 RLIKE 'b')
                  OR ((csmallint >= ctinyint)
                      AND (ctimestamp2 IS NULL))))
GROUP BY cboolean1
ORDER BY cboolean1;

