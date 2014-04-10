FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key[0], src.value
