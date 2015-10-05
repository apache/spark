-- negative, references twice for result of funcion
explain select nkey, nkey + 1 from (select key + 1 as nkey, value from src) a;

set hive.auto.convert.join=false;
-- This test query is introduced for HIVE-4968.
-- First, we do not convert the join to MapJoin.
EXPLAIN
SELECT tmp4.key as key, tmp4.value as value, tmp4.count as count
FROM (SELECT tmp2.key as key, tmp2.value as value, tmp3.count as count
      FROM (SELECT *
            FROM (SELECT key, value
                  FROM src1) tmp1 ) tmp2
      JOIN (SELECT count(*) as count
            FROM src1) tmp3
      ) tmp4 order by key, value, count;

SELECT tmp4.key as key, tmp4.value as value, tmp4.count as count
FROM (SELECT tmp2.key as key, tmp2.value as value, tmp3.count as count
      FROM (SELECT *
            FROM (SELECT key, value
                  FROM src1) tmp1 ) tmp2
      JOIN (SELECT count(*) as count
            FROM src1) tmp3
      ) tmp4 order by key, value, count;

set hive.auto.convert.join=true;
-- Then, we convert the join to MapJoin.
EXPLAIN
SELECT tmp4.key as key, tmp4.value as value, tmp4.count as count
FROM (SELECT tmp2.key as key, tmp2.value as value, tmp3.count as count
      FROM (SELECT *
            FROM (SELECT key, value
                  FROM src1) tmp1 ) tmp2
      JOIN (SELECT count(*) as count
            FROM src1) tmp3
      ) tmp4 order by key, value, count;

SELECT tmp4.key as key, tmp4.value as value, tmp4.count as count
FROM (SELECT tmp2.key as key, tmp2.value as value, tmp3.count as count
      FROM (SELECT *
            FROM (SELECT key, value
                  FROM src1) tmp1 ) tmp2
      JOIN (SELECT count(*) as count
            FROM src1) tmp3
      ) tmp4 order by key, value, count;
