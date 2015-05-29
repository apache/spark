SET hive.vectorized.execution.enabled=true;
SELECT STDDEV_SAMP(csmallint),
       (STDDEV_SAMP(csmallint) - 10.175),
       STDDEV_POP(ctinyint),
       (STDDEV_SAMP(csmallint) * (STDDEV_SAMP(csmallint) - 10.175)),
       (-(STDDEV_POP(ctinyint))),
       (STDDEV_SAMP(csmallint) % 79.553),
       (-((STDDEV_SAMP(csmallint) * (STDDEV_SAMP(csmallint) - 10.175)))),
       STDDEV_SAMP(cfloat),
       (-(STDDEV_SAMP(csmallint))),
       SUM(cfloat),
       ((-((STDDEV_SAMP(csmallint) * (STDDEV_SAMP(csmallint) - 10.175)))) / (STDDEV_SAMP(csmallint) - 10.175)),
       (-((STDDEV_SAMP(csmallint) - 10.175))),
       AVG(cint),
       (-3728 - STDDEV_SAMP(csmallint)),
       STDDEV_POP(cint),
       (AVG(cint) / STDDEV_SAMP(cfloat))
FROM   alltypesorc
WHERE  (((cint <= cfloat)
         AND ((79.553 != cbigint)
              AND (ctimestamp2 = -29071)))
        OR ((cbigint > cdouble)
            AND ((79.553 <= csmallint)
                 AND (ctimestamp1 > ctimestamp2))));

