SET hive.vectorized.execution.enabled=true;
SELECT VAR_POP(ctinyint),
       (VAR_POP(ctinyint) / -26.28),
       SUM(cfloat),
       (-1.389 + SUM(cfloat)),
       (SUM(cfloat) * (-1.389 + SUM(cfloat))),
       MAX(ctinyint),
       (-((SUM(cfloat) * (-1.389 + SUM(cfloat))))),
       MAX(cint),
       (MAX(cint) * 79.553),
       VAR_SAMP(cdouble),
       (10.175 % (-((SUM(cfloat) * (-1.389 + SUM(cfloat)))))),
       COUNT(cint),
       (-563 % MAX(cint))
FROM   alltypesorc
WHERE  (((cdouble > ctinyint)
         AND (cboolean2 > 0))
        OR ((cbigint < ctinyint)
            OR ((cint > cbigint)
                OR (cboolean1 < 0))));

