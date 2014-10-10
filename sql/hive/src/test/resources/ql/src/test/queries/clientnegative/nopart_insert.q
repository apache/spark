
CREATE TABLE nopart_insert(a STRING, b STRING) PARTITIONED BY (ds STRING);

INSERT OVERWRITE TABLE nopart_insert 
SELECT TRANSFORM(src.key, src.value) USING '../data/scripts/error_script' AS (tkey, tvalue)
FROM src;

