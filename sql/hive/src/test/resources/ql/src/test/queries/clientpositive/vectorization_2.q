SET hive.vectorized.execution.enabled=true;
SELECT AVG(csmallint),
       (AVG(csmallint) % -563),
       (AVG(csmallint) + 762),
       SUM(cfloat),
       VAR_POP(cbigint),
       (-(VAR_POP(cbigint))),
       (SUM(cfloat) - AVG(csmallint)),
       COUNT(*),
       (-((SUM(cfloat) - AVG(csmallint)))),
       (VAR_POP(cbigint) - 762),
       MIN(ctinyint),
       ((-(VAR_POP(cbigint))) + MIN(ctinyint)),
       AVG(cdouble),
       (((-(VAR_POP(cbigint))) + MIN(ctinyint)) - SUM(cfloat))
FROM   alltypesorc
WHERE  (((ctimestamp1 < ctimestamp2)
         AND ((cstring2 LIKE 'b%')
              AND (cfloat <= -5638.15)))
        OR ((cdouble < ctinyint)
            AND ((-10669 != ctimestamp2)
                 OR (359 > cint))));

