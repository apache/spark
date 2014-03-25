FROM UNIQUEJOIN src a (a.key), PRESERVE src b (b.key) JOIN src c ON c.key
SELECT a.key;

