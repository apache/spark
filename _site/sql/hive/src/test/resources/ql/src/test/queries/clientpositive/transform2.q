-- Transform with a function that has many parameters
SELECT TRANSFORM(substr(key, 1, 2)) USING 'cat' FROM src LIMIT 1;
