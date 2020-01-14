CREATE TABLE table_with_comment (a STRING, b INT, c STRING, d STRING) USING parquet COMMENT 'added';

DESC FORMATTED table_with_comment;

-- ALTER TABLE BY MODIFYING COMMENT
ALTER TABLE table_with_comment SET TBLPROPERTIES("comment"= "modified comment", "type"= "parquet");

DESC FORMATTED table_with_comment;

-- DROP TEST TABLE
DROP TABLE table_with_comment;

-- CREATE TABLE WITHOUT COMMENT
CREATE TABLE table_comment (a STRING, b INT) USING parquet;

DESC FORMATTED table_comment;

-- ALTER TABLE BY ADDING COMMENT
ALTER TABLE table_comment SET TBLPROPERTIES(comment = "added comment");

DESC formatted table_comment;

-- ALTER UNSET PROPERTIES COMMENT
ALTER TABLE table_comment UNSET TBLPROPERTIES IF EXISTS ('comment');

DESC FORMATTED table_comment;

-- DROP TEST TABLE
DROP TABLE table_comment;
