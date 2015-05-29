FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key, DISTINCT substr(src.value,4,1) GROUP BY src.key
