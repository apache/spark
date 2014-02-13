-- test comment indent processing for multi-line comments

CREATE TABLE test_table(
    col1 INT COMMENT 'col1 one line comment',
    col2 STRING COMMENT 'col2
two lines comment',
    col3 STRING COMMENT 'col3
three lines
comment',
    col4 STRING COMMENT 'col4 very long comment that is greater than 80 chars and is likely to spill into multiple lines',
    col5 STRING COMMENT 'col5 very long multi-line comment where each line is very long by itself and is likely to spill
into multiple lines.  Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin in dolor nisl, sodales
adipiscing tortor. Integer venenatis',
    col6 STRING COMMENT 'This comment has a very long single word ABCDEFGHIJKLMNOPQRSTUVXYZabcdefghijklmnopqrstuvzxyz123 which will not fit in a line by itself for small column widths.',
    col7_NoComment STRING)
COMMENT 'table comment
two lines';

SET hive.cli.pretty.output.num.cols=80;

-- There will be an extra tab at the end of each comment line in the output.
-- This is because DESCRIBE <table_name> command separates the column, type and
-- comment field using a \t. DESCRIBE PRETTY <table_name> uses spaces instead 
-- of \t to separate columns. Hive gets confused when it parses the string
-- table description constructed in MetaDataPrettyFormatUtils, and adds a tab
-- at the end of each line.
-- There are three ways to address this:
-- 1. Pad each row to the full terminal width with extra spaces. 
-- 2. Assume a maximum tab width of 8, and subtract 2 * 8 spaces from the 
--    available line width. This approach wastes upto 2 * 8 - 2 columns.
-- 3. Since the pretty output is meant only for human consumption, do nothing. 
--    Just add a comment to the unit test file explaining what is happening.
--    This is the approach chosen.

DESCRIBE PRETTY test_table;

SET hive.cli.pretty.output.num.cols=200;
DESCRIBE PRETTY test_table;

SET hive.cli.pretty.output.num.cols=50;
DESCRIBE PRETTY test_table;

SET hive.cli.pretty.output.num.cols=60;
DESCRIBE PRETTY test_table;

CREATE TABLE test_table_very_long_column_name(
    col1 INT COMMENT 'col1 one line comment',
    col2_abcdefghiklmnopqrstuvxyz STRING COMMENT 'col2
two lines comment',
    col3 STRING COMMENT 'col3
three lines
comment',
    col4 STRING COMMENT 'col4 very long comment that is greater than 80 chars and is likely to spill into multiple lines')
;

SET hive.cli.pretty.output.num.cols=80;
DESCRIBE PRETTY test_table_very_long_column_name;

SET hive.cli.pretty.output.num.cols=20;
DESCRIBE PRETTY test_table_very_long_column_name;

CREATE TABLE test_table_partitioned(
    col1 INT COMMENT 'col1 one line comment',
    col2 STRING COMMENT 'col2
two lines comment',
    col3 STRING COMMENT 'col3
three lines
comment',
    col4 STRING COMMENT 'col4 very long comment that is greater than 80 chars and is likely to spill into multiple lines',
    col5 STRING COMMENT 'col5 very long multi-line comment where each line is very long by itself and is likely to spill
into multiple lines.  Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin in dolor nisl, sodales
adipiscing tortor. Integer venenatis',
    col6 STRING COMMENT 'This comment has a very long single word ABCDEFGHIJKLMNOPQRSTUVXYZabcdefghijklmnopqrstuvzxyz123 which will not fit in a line by itself for small column widths.',
    col7_NoComment STRING)
COMMENT 'table comment
two lines'
PARTITIONED BY (ds STRING);

SET hive.cli.pretty.output.num.cols=60;
DESCRIBE PRETTY test_table_partitioned;
