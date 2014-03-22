FROM src
INSERT OVERWRITE TABLE dest1 SELECT '1234', dummyfn(src.value, 10) WHERE src.key < 100
