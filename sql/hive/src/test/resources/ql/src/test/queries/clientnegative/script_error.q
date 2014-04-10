EXPLAIN
SELECT TRANSFORM(src.key, src.value) USING '../data/scripts/error_script' AS (tkey, tvalue)
FROM src;

SELECT TRANSFORM(src.key, src.value) USING '../data/scripts/error_script' AS (tkey, tvalue)
FROM src;

