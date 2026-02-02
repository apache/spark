select INPUT__FILE__NAME, key, BLOCK__OFFSET__INSIDE__FILE from src;

select key, count(INPUT__FILE__NAME) from src group by key order by key;

select INPUT__FILE__NAME, key, collect_set(BLOCK__OFFSET__INSIDE__FILE) from src group by INPUT__FILE__NAME, key order by key;

select * from src where BLOCK__OFFSET__INSIDE__FILE > 12000 order by key;

select * from src where BLOCK__OFFSET__INSIDE__FILE > 5800 order by key;


CREATE TABLE src_index_test_rc (key int, value string) STORED AS RCFILE;

set hive.io.rcfile.record.buffer.size = 1024;
INSERT OVERWRITE TABLE src_index_test_rc SELECT * FROM src;
select INPUT__FILE__NAME, key, BLOCK__OFFSET__INSIDE__FILE from src_index_test_rc order by key;

DROP TABLE src_index_test_rc;
DROP INDEX src_index on src_index_test_rc;
