DROP VIEW xxx14;

-- Ideally (and according to SQL:200n), this should actually be legal,
-- but since internally we impose the new column descriptors by
-- reference to underlying name rather than position, we have to make
-- it illegal.  There's an easy workaround (provide the unique names
-- via direct column aliases, e.g. SELECT key AS x, key AS y)
CREATE VIEW xxx14(x,y) AS
SELECT key,key FROM src;
