SET hive.vectorized.execution.enabled=true;
SELECT SUM(cint),
       (SUM(cint) * -563),
       (-3728 + SUM(cint)),
       STDDEV_POP(cdouble),
       (-(STDDEV_POP(cdouble))),
       AVG(cdouble),
       ((SUM(cint) * -563) % SUM(cint)),
       (((SUM(cint) * -563) % SUM(cint)) / AVG(cdouble)),
       VAR_POP(cdouble),
       (-((((SUM(cint) * -563) % SUM(cint)) / AVG(cdouble)))),
       ((-3728 + SUM(cint)) - (SUM(cint) * -563)),
       MIN(ctinyint),
       MIN(ctinyint),
       (MIN(ctinyint) * (-((((SUM(cint) * -563) % SUM(cint)) / AVG(cdouble)))))
FROM   alltypesorc
WHERE  (((csmallint >= cint)
         OR ((-89010 >= ctinyint)
             AND (cdouble > 79.553)))
        OR ((-563 != cbigint)
            AND ((ctinyint != cbigint)
                 OR (-3728 >= cdouble))));

