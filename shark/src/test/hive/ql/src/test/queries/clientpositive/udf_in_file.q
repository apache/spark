DESCRIBE FUNCTION in_file;

EXPLAIN
SELECT in_file("303", "../data/files/test2.dat"),
       in_file("304", "../data/files/test2.dat"),
       in_file(CAST(NULL AS STRING), "../data/files/test2.dat")
FROM src LIMIT 1;

SELECT in_file("303", "../data/files/test2.dat"),
       in_file("304", "../data/files/test2.dat"),
       in_file(CAST(NULL AS STRING), "../data/files/test2.dat")
FROM src LIMIT 1;
