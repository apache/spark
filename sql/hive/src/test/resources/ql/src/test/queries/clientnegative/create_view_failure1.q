
DROP VIEW xxx12;

-- views and tables share the same namespace
CREATE TABLE xxx12(key int);
CREATE VIEW xxx12 AS SELECT key FROM src;
