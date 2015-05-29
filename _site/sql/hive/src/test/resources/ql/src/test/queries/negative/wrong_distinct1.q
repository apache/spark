FROM src
INSERT OVERWRITE TABLE dest1 SELECT DISTINCT src.key, substr(src.value,4,1) GROUP BY src.key
