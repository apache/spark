CREATE TABLE table(string string) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../data/files/docurl.txt' INTO TABLE table;

SELECT table, count(1)
FROM
(
  FROM table
  SELECT TRANSFORM (table.string)
  USING 'java -cp ../build/ql/test/classes org.apache.hadoop.hive.scripts.extracturl' AS (table, count)
) subq
GROUP BY table;
