FROM src
INSERT OVERWRITE TABLE dest1 SELECT substr('1234', 'abc'), src.value WHERE src.key < 100
