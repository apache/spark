DROP VIEW xxx5;

-- duplicate column names are illegal
CREATE VIEW xxx5(x,x) AS
SELECT key,value FROM src;
