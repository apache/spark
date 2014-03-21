DROP VIEW xxx13;

-- number of explicit view column defs must match underlying SELECT
CREATE VIEW xxx13(x,y,z) AS
SELECT key FROM src;
