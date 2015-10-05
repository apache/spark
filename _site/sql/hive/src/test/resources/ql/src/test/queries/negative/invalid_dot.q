FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.value.member WHERE src.key < 100
