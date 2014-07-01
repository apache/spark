FROM src
INSERT OVERWRITE TABLE dest1 SELECT '1234', src.value WHERE anotherdummyfn('abc', src.key) + 10 < 100 group by src.key
