DROP VIEW av;

CREATE VIEW av AS SELECT * FROM src;

-- should fail:  can't analyze a view...yet
ANALYZE TABLE av COMPUTE STATISTICS;
