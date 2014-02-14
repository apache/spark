set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

EXPLAIN
FROM (
  FROM src select src.key, src.value WHERE src.key < '100'
    UNION ALL
  FROM src SELECT src.* WHERE src.key > '150'
) unioned_query
SELECT unioned_query.*
  WHERE key > '4' and value > 'val_4';

FROM (
  FROM src select src.key, src.value WHERE src.key < '100'
    UNION ALL
  FROM src SELECT src.* WHERE src.key > '150'
) unioned_query
SELECT unioned_query.*
  WHERE key > '4' and value > 'val_4';

set hive.ppd.remove.duplicatefilters=true;

EXPLAIN
FROM (
  FROM src select src.key, src.value WHERE src.key < '100'
    UNION ALL
  FROM src SELECT src.* WHERE src.key > '150'
) unioned_query
SELECT unioned_query.*
  WHERE key > '4' and value > 'val_4';

FROM (
  FROM src select src.key, src.value WHERE src.key < '100'
    UNION ALL
  FROM src SELECT src.* WHERE src.key > '150'
) unioned_query
SELECT unioned_query.*
  WHERE key > '4' and value > 'val_4';
