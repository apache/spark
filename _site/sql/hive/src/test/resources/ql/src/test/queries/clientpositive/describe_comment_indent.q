-- test comment indent processing for multi-line comments

CREATE TABLE test_table(
    col1 INT COMMENT 'col1 one line comment',
    col2 STRING COMMENT 'col2
two lines comment',
    col3 STRING COMMENT 'col3
three lines
comment') 
COMMENT 'table comment
two lines';

DESCRIBE test_table;
DESCRIBE FORMATTED test_table;
