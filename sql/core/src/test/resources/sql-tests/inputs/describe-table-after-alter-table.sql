CREATE TABLE table_with_comment (a STRING, b INT, c STRING, d STRING) USING parquet COMMENT 'added';

DESC FORMATTED table_with_comment;

-- ALTER TABLE BY MODIFYING COMMENT
COMMENT ON TABLE table_with_comment IS 'modified comment';

DESC FORMATTED table_with_comment;

-- DROP TEST TABLE
DROP TABLE table_with_comment;

-- CREATE TABLE WITHOUT COMMENT
CREATE TABLE table_comment (a STRING, b INT) USING parquet;

DESC FORMATTED table_comment;

-- ALTER TABLE BY ADDING COMMENT
COMMENT ON TABLE table_comment IS 'added comment';

DESC formatted table_comment;

-- ALTER UNSET PROPERTIES COMMENT
COMMENT ON TABLE table_comment IS NULL ;

DESC FORMATTED table_comment;

-- DROP TEST TABLE
DROP TABLE table_comment;
