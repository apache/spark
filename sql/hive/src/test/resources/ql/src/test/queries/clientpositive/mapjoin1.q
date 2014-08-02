SELECT  /*+ MAPJOIN(b) */ sum(a.key) as sum_a
    FROM srcpart a
    JOIN src b ON a.key = b.key where a.ds is not null;

set hive.outerjoin.supports.filters=true;

-- const filter on outer join
EXPLAIN
SELECT /*+ MAPJOIN(a) */ * FROM src a RIGHT OUTER JOIN src b on a.key=b.key AND true limit 10;
SELECT /*+ MAPJOIN(a) */ * FROM src a RIGHT OUTER JOIN src b on a.key=b.key AND true limit 10;

-- func filter on outer join
EXPLAIN
SELECT /*+ MAPJOIN(a) */ * FROM src a RIGHT OUTER JOIN src b on a.key=b.key AND b.key * 10 < '1000' limit 10;
SELECT /*+ MAPJOIN(a) */ * FROM src a RIGHT OUTER JOIN src b on a.key=b.key AND b.key * 10 < '1000' limit 10;

-- field filter on outer join
EXPLAIN
SELECT /*+ MAPJOIN(a) */ * FROM src a RIGHT OUTER JOIN
    (select key, named_struct('key', key, 'value', value) as kv from src) b on a.key=b.key AND b.kv.key > 200 limit 10;
SELECT /*+ MAPJOIN(a) */ * FROM src a RIGHT OUTER JOIN
    (select key, named_struct('key', key, 'value', value) as kv from src) b on a.key=b.key AND b.kv.key > 200 limit 10;

set hive.outerjoin.supports.filters=false;

EXPLAIN
SELECT /*+ MAPJOIN(a) */ * FROM src a RIGHT OUTER JOIN src b on a.key=b.key AND true limit 10;
SELECT /*+ MAPJOIN(a) */ * FROM src a RIGHT OUTER JOIN src b on a.key=b.key AND true limit 10;

EXPLAIN
SELECT /*+ MAPJOIN(a) */ * FROM src a RIGHT OUTER JOIN src b on a.key=b.key AND b.key * 10 < '1000' limit 10;
SELECT /*+ MAPJOIN(a) */ * FROM src a RIGHT OUTER JOIN src b on a.key=b.key AND b.key * 10 < '1000' limit 10;

EXPLAIN
SELECT /*+ MAPJOIN(a) */ * FROM src a RIGHT OUTER JOIN
    (select key, named_struct('key', key, 'value', value) as kv from src) b on a.key=b.key AND b.kv.key > 200 limit 10;
SELECT /*+ MAPJOIN(a) */ * FROM src a RIGHT OUTER JOIN
    (select key, named_struct('key', key, 'value', value) as kv from src) b on a.key=b.key AND b.kv.key > 200 limit 10;
