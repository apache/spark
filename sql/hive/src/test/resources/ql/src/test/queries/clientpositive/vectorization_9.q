SET hive.vectorized.execution.enabled=true;
SELECT   cfloat,
         cstring1,
         cint,
         ctimestamp1,
         cdouble,
         cbigint,
         (cfloat / ctinyint),
         (cint % cbigint),
         (-(cdouble)),
         (cdouble + (cfloat / ctinyint)),
         (cdouble / cint),
         (-((-(cdouble)))),
         (9763215.5639 % cbigint),
         (2563.58 + (-((-(cdouble)))))
FROM     alltypesorc
WHERE    (((cbigint > -23)
           AND ((cdouble != 988888)
                OR (cint > -863.257)))
          AND ((ctinyint >= 33)
              OR ((csmallint >= cbigint)
                  OR (cfloat = cdouble))))
ORDER BY cbigint, cfloat;

