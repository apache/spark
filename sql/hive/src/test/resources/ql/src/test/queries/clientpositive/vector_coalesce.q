SET hive.vectorized.execution.enabled=true;
EXPLAIN SELECT cdouble, cstring1, cint, cfloat, csmallint, coalesce(cdouble, cstring1, cint, cfloat, csmallint) 
FROM alltypesorc
WHERE (cdouble IS NULL) LIMIT 10;

SELECT cdouble, cstring1, cint, cfloat, csmallint, coalesce(cdouble, cstring1, cint, cfloat, csmallint) 
FROM alltypesorc
WHERE (cdouble IS NULL) LIMIT 10;

EXPLAIN SELECT ctinyint, cdouble, cint, coalesce(ctinyint+10, (cdouble+log2(cint)), 0) 
FROM alltypesorc
WHERE (ctinyint IS NULL) LIMIT 10;

SELECT ctinyint, cdouble, cint, coalesce(ctinyint+10, (cdouble+log2(cint)), 0) 
FROM alltypesorc
WHERE (ctinyint IS NULL) LIMIT 10;

EXPLAIN SELECT cfloat, cbigint, coalesce(cfloat, cbigint, 0) 
FROM alltypesorc
WHERE (cfloat IS NULL AND cbigint IS NULL) LIMIT 10;

SELECT cfloat, cbigint, coalesce(cfloat, cbigint, 0) 
FROM alltypesorc
WHERE (cfloat IS NULL AND cbigint IS NULL) LIMIT 10;

EXPLAIN SELECT ctimestamp1, ctimestamp2, coalesce(ctimestamp1, ctimestamp2) 
FROM alltypesorc 
WHERE ctimestamp1 IS NOT NULL OR ctimestamp2 IS NOT NULL LIMIT 10;

SELECT ctimestamp1, ctimestamp2, coalesce(ctimestamp1, ctimestamp2) 
FROM alltypesorc 
WHERE ctimestamp1 IS NOT NULL OR ctimestamp2 IS NOT NULL LIMIT 10;
