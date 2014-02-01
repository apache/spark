FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key, src.value, 1 WHERE src.key < 100
