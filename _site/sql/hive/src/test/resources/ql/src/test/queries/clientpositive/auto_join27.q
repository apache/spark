set hive.auto.convert.join = true;

explain
SELECT count(1)
FROM
(
SELECT src.key, src.value from src
UNION ALL
SELECT DISTINCT src.key, src.value from src
) src_12
JOIN
(SELECT src.key as k, src.value as v from src) src3
ON src_12.key = src3.k AND src3.k < 200;


SELECT count(1)
FROM
(
SELECT src.key, src.value from src
UNION ALL
SELECT DISTINCT src.key, src.value from src
) src_12
JOIN
(SELECT src.key as k, src.value as v from src) src3
ON src_12.key = src3.k AND src3.k < 200;
