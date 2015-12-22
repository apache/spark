SET hive.vectorized.execution.enabled=true;
SELECT MAX(csmallint),
       (MAX(csmallint) * -75),
       COUNT(*),
       ((MAX(csmallint) * -75) / COUNT(*)),
       (6981 * MAX(csmallint)),
       MIN(csmallint),
       (-(MIN(csmallint))),
       (197 % ((MAX(csmallint) * -75) / COUNT(*))),
       SUM(cint),
       MAX(ctinyint),
       (-(MAX(ctinyint))),
       ((-(MAX(ctinyint))) + MAX(ctinyint))
FROM   alltypesorc
WHERE  (((cboolean2 IS NOT NULL)
         AND (cstring1 LIKE '%b%'))
        OR ((ctinyint = cdouble)
            AND ((ctimestamp2 IS NOT NULL)
                 AND (cstring2 LIKE 'a'))));

