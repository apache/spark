DROP VIEW xxx8;
DROP VIEW xxx9;

-- create two levels of view reference, then invalidate intermediate view
-- by dropping a column from underlying table, and verify that
-- querying outermost view results in full error context
CREATE TABLE xxx10 (key int, value int);
CREATE VIEW xxx9 AS SELECT * FROM xxx10;
CREATE VIEW xxx8 AS SELECT * FROM xxx9 xxx;
ALTER TABLE xxx10 REPLACE COLUMNS (key int);
SELECT * FROM xxx8 yyy;
