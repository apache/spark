SET hive.vectorized.execution.enabled=true;
SELECT ctimestamp1,
       cdouble,
       cboolean1,
       cstring1,
       cfloat,
       (-(cdouble)),
       (-5638.15 - cdouble),
       (cdouble * -257),
       (cint + cfloat),
       ((-(cdouble)) + cbigint),
       (-(cdouble)),
       (-1.389 - cfloat),
       (-(cfloat)),
       ((-5638.15 - cdouble) + (cint + cfloat))
FROM   alltypesorc
WHERE  (((cstring2 IS NOT NULL)
         AND ((ctimestamp1 <= -29071)
             AND (ctimestamp2 != 16558)))
        OR ((cfloat < -6432)
            OR ((cboolean1 IS NOT NULL)
                AND (cdouble = 988888))));

