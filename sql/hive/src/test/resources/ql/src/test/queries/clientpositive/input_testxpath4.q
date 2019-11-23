set hive.optimize.ppd=false;

EXPLAIN
FROM src_thrift
SELECT src_thrift.mstringstring['key_9'], lintstring.myint
WHERE src_thrift.mstringstring['key_9'] IS NOT NULL
      AND lintstring.myint IS NOT NULL
      AND lintstring IS NOT NULL;

FROM src_thrift
SELECT src_thrift.mstringstring['key_9'], lintstring.myint
WHERE src_thrift.mstringstring['key_9'] IS NOT NULL
      OR lintstring.myint IS NOT NULL
      OR lintstring IS NOT NULL;

set hive.optimize.ppd=true;

EXPLAIN
FROM src_thrift
SELECT src_thrift.mstringstring['key_9'], lintstring.myint
WHERE src_thrift.mstringstring['key_9'] IS NOT NULL
      AND lintstring.myint IS NOT NULL
      AND lintstring IS NOT NULL;

FROM src_thrift
SELECT src_thrift.mstringstring['key_9'], lintstring.myint
WHERE src_thrift.mstringstring['key_9'] IS NOT NULL
      OR lintstring.myint IS NOT NULL
      OR lintstring IS NOT NULL;
