SET hive.vectorized.execution.enabled=true;
SELECT   cfloat,
         cboolean1,
         cdouble,
         cstring1,
         ctinyint,
         cint,
         ctimestamp1,
         STDDEV_SAMP(cfloat),
         (-26.28 - cint),
         MIN(cdouble),
         (cdouble * 79.553),
         (33 % cfloat),
         STDDEV_SAMP(ctinyint),
         VAR_POP(ctinyint),
         (-23 % cdouble),
         (-(ctinyint)),
         VAR_SAMP(cint),
         (cint - cfloat),
         (-23 % ctinyint),
         (-((-26.28 - cint))),
         STDDEV_POP(cint)
FROM     alltypesorc
WHERE    (((cstring2 LIKE '%ss%')
           OR (cstring1 LIKE '10%'))
          OR ((cint >= -75)
              AND ((ctinyint = csmallint)
                   AND (cdouble >= -3728))))
GROUP BY cfloat, cboolean1, cdouble, cstring1, ctinyint, cint, ctimestamp1
ORDER BY cfloat, cboolean1, cdouble, cstring1, ctinyint, cint, ctimestamp1;

