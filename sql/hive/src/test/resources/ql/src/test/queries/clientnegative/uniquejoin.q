FROM UNIQUEJOIN (SELECT src.key from src WHERE src.key<4) a (a.key), PRESERVE  src b(b.key)
SELECT a.key, b.key;

