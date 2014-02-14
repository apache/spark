FROM src1
INSERT OVERWRITE TABLE dest1 SELECT src1.key, src1.value WHERE src1.key is null
