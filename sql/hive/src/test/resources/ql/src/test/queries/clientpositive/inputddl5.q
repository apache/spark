-- test for internationalization
-- kv4.txt contains the utf-8 character 0xE982B5E993AE which we are verifying later on
CREATE TABLE INPUTDDL5(name STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv4.txt' INTO TABLE INPUTDDL5;
DESCRIBE INPUTDDL5;
SELECT INPUTDDL5.name from INPUTDDL5;
SELECT count(1) FROM INPUTDDL5 WHERE INPUTDDL5.name = _UTF-8 0xE982B5E993AE;

