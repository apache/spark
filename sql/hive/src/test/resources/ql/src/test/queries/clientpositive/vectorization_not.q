SET hive.vectorized.execution.enabled=true;
SELECT AVG(cbigint),
       (-(AVG(cbigint))),
       (-6432 + AVG(cbigint)),
       STDDEV_POP(cbigint),
       (-((-6432 + AVG(cbigint)))),
       ((-((-6432 + AVG(cbigint)))) + (-6432 + AVG(cbigint))),
       VAR_SAMP(cbigint),
       (-((-6432 + AVG(cbigint)))),
       (-6432 + (-((-6432 + AVG(cbigint))))),
       (-((-6432 + AVG(cbigint)))),
       ((-((-6432 + AVG(cbigint)))) / (-((-6432 + AVG(cbigint))))),
       COUNT(*),
       SUM(cfloat),
       (VAR_SAMP(cbigint) % STDDEV_POP(cbigint)),
       (-(VAR_SAMP(cbigint))),
       ((-((-6432 + AVG(cbigint)))) * (-(AVG(cbigint)))),
       MIN(ctinyint),
       (-(MIN(ctinyint)))
FROM   alltypesorc
WHERE  (((cstring2 LIKE '%b%')
         OR ((79.553 != cint)
             OR (NOT(cbigint >= cdouble))))
        OR ((ctinyint >= csmallint)
            AND (NOT ((cboolean2 != 1)
                 OR (3569 != ctinyint)))));

