GRANT ALL TO USER hive_test_user;
SET hive.security.authorization.enabled=true;
CREATE TABLE src_authorization_7 (key int, value string);
DESCRIBE src_authorization_7;
DROP TABLE  src_authorization_7;
REVOKE ALL FROM USER hive_test_user;

SET hive.security.authorization.enabled=false;

GRANT ALL TO GROUP hive_test_group1;
SET hive.security.authorization.enabled=true;
CREATE TABLE src_authorization_7 (key int, value string);
DESCRIBE src_authorization_7;
DROP TABLE  src_authorization_7;
REVOKE ALL FROM GROUP hive_test_group1;