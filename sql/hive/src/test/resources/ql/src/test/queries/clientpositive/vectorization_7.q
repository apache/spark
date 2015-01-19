SET hive.vectorized.execution.enabled=true;
SELECT cboolean1,
       cbigint,
       csmallint,
       ctinyint,
       ctimestamp1,
       cstring1,
       (cbigint + cbigint),
       (csmallint % -257),
       (-(csmallint)),
       (-(ctinyint)),
       ((-(ctinyint)) + 17),
       (cbigint * (-(csmallint))),
       (cint % csmallint),
       (-(ctinyint)),
       ((-(ctinyint)) % ctinyint)
FROM   alltypesorc
WHERE  ((ctinyint != 0)
        AND (((ctimestamp1 <= 0)
          OR ((ctinyint = cint)
               OR (cstring2 LIKE 'ss')))
          AND ((988888 < cdouble)
              OR ((ctimestamp2 > -29071)
                  AND (3569 >= cdouble)))));

