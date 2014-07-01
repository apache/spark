FROM src
INSERT OVERWRITE TABLE dest4_sequencefile SELECT src.key, src.value
