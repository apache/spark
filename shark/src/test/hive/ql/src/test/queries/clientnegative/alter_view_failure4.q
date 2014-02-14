DROP VIEW xxx5;
CREATE VIEW xxx5
PARTITIONED ON (value)
AS 
SELECT * FROM src;

-- should fail:  LOCATION clause is illegal
ALTER VIEW xxx5 ADD PARTITION (value='val_86') LOCATION '/foo/bar/baz';
