-- HIVE-2905 showing non-ascii comments

create table dummy (col1 string, col2 string, col3 string);

alter table dummy change col1 col1 string comment '한글_col1';
alter table dummy change col2 col2 string comment '漢字_col2';
alter table dummy change col3 col3 string comment 'わご_col3';

DESCRIBE FORMATTED dummy;
