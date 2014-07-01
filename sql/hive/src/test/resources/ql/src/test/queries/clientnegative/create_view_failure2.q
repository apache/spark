
DROP VIEW xxx4;

-- views and tables share the same namespace
CREATE VIEW xxx4 AS SELECT key FROM src;
CREATE TABLE xxx4(key int);
