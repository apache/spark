create table testTable(col1 int, col2 int);

-- set a table property = null, it should be caught by the grammar
alter table testTable set tblproperties ('a'=);
