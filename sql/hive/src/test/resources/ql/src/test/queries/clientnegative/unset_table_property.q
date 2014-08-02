CREATE TABLE testTable(col1 INT, col2 INT);
ALTER TABLE testTable SET TBLPROPERTIES ('a'='1', 'c'='3');
SHOW TBLPROPERTIES testTable;

-- unset a subset of the properties and some non-existed properties without if exists
ALTER TABLE testTable UNSET TBLPROPERTIES ('c', 'x', 'y', 'z');
