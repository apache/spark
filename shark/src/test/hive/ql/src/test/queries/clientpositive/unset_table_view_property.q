CREATE TABLE testTable(col1 INT, col2 INT);
SHOW TBLPROPERTIES testTable;

-- UNSET TABLE PROPERTIES
ALTER TABLE testTable SET TBLPROPERTIES ('a'='1', 'c'='3');
SHOW TBLPROPERTIES testTable;

-- UNSET all the properties
ALTER TABLE testTable UNSET TBLPROPERTIES ('a', 'c');
SHOW TBLPROPERTIES testTable;

ALTER TABLE testTable SET TBLPROPERTIES ('a'='1', 'c'='3', 'd'='4');
SHOW TBLPROPERTIES testTable;

-- UNSET a subset of the properties
ALTER TABLE testTable UNSET TBLPROPERTIES ('a', 'd');
SHOW TBLPROPERTIES testTable;

-- the same property being UNSET multiple times
ALTER TABLE testTable UNSET TBLPROPERTIES ('c', 'c', 'c');
SHOW TBLPROPERTIES testTable;

ALTER TABLE testTable SET TBLPROPERTIES ('a'='1', 'b' = '2', 'c'='3', 'd'='4');
SHOW TBLPROPERTIES testTable;

-- UNSET a subset of the properties and some non-existed properties using IF EXISTS
ALTER TABLE testTable UNSET TBLPROPERTIES IF EXISTS ('b', 'd', 'b', 'f');
SHOW TBLPROPERTIES testTable;

-- UNSET a subset of the properties and some non-existed properties using IF EXISTS
ALTER TABLE testTable UNSET TBLPROPERTIES IF EXISTS ('b', 'd', 'c', 'f', 'x', 'y', 'z');
SHOW TBLPROPERTIES testTable;

-- UNSET VIEW PROPERTIES
CREATE VIEW testView AS SELECT value FROM src WHERE key=86;
ALTER VIEW testView SET TBLPROPERTIES ('propA'='100', 'propB'='200');
SHOW TBLPROPERTIES testView;

-- UNSET all the properties
ALTER VIEW testView UNSET TBLPROPERTIES ('propA', 'propB');
SHOW TBLPROPERTIES testView;

ALTER VIEW testView SET TBLPROPERTIES ('propA'='100', 'propC'='300', 'propD'='400');
SHOW TBLPROPERTIES testView;

-- UNSET a subset of the properties
ALTER VIEW testView UNSET TBLPROPERTIES ('propA', 'propC');
SHOW TBLPROPERTIES testView;

-- the same property being UNSET multiple times
ALTER VIEW testView UNSET TBLPROPERTIES ('propD', 'propD', 'propD');
SHOW TBLPROPERTIES testView;

ALTER VIEW testView SET TBLPROPERTIES ('propA'='100', 'propB' = '200', 'propC'='300', 'propD'='400');
SHOW TBLPROPERTIES testView;

-- UNSET a subset of the properties and some non-existed properties using IF EXISTS
ALTER VIEW testView UNSET TBLPROPERTIES IF EXISTS ('propC', 'propD', 'propD', 'propC', 'propZ');
SHOW TBLPROPERTIES testView;

-- UNSET a subset of the properties and some non-existed properties using IF EXISTS
ALTER VIEW testView UNSET TBLPROPERTIES IF EXISTS ('propB', 'propC', 'propD', 'propF');
SHOW TBLPROPERTIES testView;

