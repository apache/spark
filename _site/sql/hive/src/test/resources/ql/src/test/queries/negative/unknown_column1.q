FROM src
INSERT OVERWRITE TABLE dest1 SELECT '1234', src.dummycol WHERE src.key < 100
