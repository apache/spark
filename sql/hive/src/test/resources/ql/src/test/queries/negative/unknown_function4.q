FROM src
INSERT OVERWRITE TABLE dest1 SELECT '1234', dummyfn(src.key) WHERE src.key < 100 group by src.key
