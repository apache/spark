create table documents(contents string) stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/docurl.txt' INTO TABLE documents;


select url, count(1) 
FROM
(
  FROM documents
  MAP documents.contents
  USING 'java -cp ../util/target/classes/ org.apache.hadoop.hive.scripts.extracturl' AS (url, count)
) subq
group by url;



