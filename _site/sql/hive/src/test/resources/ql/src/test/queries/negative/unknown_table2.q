FROM src
INSERT OVERWRITE TABLE dummyDest SELECT '1234', src.value WHERE src.key < 100
