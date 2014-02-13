FROM src
INSERT TABLE dest1 SELECT '1234', src.value WHERE src.key < 100
