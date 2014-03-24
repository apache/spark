FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key, sum(substr(src.value,5)) GROUP BY src.key
