SET hive.vectorized.execution.enabled=true;
SELECT cstring1,
       cboolean1,
       cdouble,
       ctimestamp1,
       (-3728 * csmallint),
       (cdouble - 9763215.5639),
       (-(cdouble)),
       ((-(cdouble)) + 6981),
       (cdouble * -5638.15)
FROM   alltypesorc
WHERE  ((cstring2 = cstring1)
        OR ((ctimestamp1 IS NULL)
            AND (cstring1 LIKE '%a')));

