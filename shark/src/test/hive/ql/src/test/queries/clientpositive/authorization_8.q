set hive.security.authorization.enabled=true;
GRANT ALL TO USER hive_test_user;
CREATE TABLE tbl_j5jbymsx8e (key INT, value STRING) PARTITIONED BY (ds STRING);
CREATE VIEW view_j5jbymsx8e_1 as SELECT * FROM tbl_j5jbymsx8e;
DESCRIBE view_j5jbymsx8e_1;
ALTER VIEW view_j5jbymsx8e_1 RENAME TO view_j5jbymsx8e_2;
REVOKE ALL FROM USER hive_test_user;
set hive.security.authorization.enabled=false;
