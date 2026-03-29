-- Test for named arguments for Mask
SELECT mask('AbCD123-@$#', lowerChar => 'q', upperChar => 'Q', otherChar => 'o', digitChar => 'd');
SELECT mask(lowerChar => 'q', upperChar => 'Q', otherChar => 'o', digitChar => 'd', str => 'AbCD123-@$#');
SELECT mask('AbCD123-@$#', lowerChar => 'q', upperChar => 'Q', digitChar => 'd');
SELECT mask(lowerChar => 'q', upperChar => 'Q', digitChar => 'd', str => 'AbCD123-@$#');

-- Test for named arguments for CountMinSketchAgg
create temporary view t2 as select * from values
  ('val2a', 6S, 12, 14L, float(15), 20D, 20E2, timestamp '2014-04-04 01:01:00.000', date '2014-04-04'),
  ('val1b', 10S, 12, 19L, float(17), 25D, 26E2, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ('val1b', 8S, 16, 119L, float(17), 25D, 26E2, timestamp '2015-05-04 01:01:00.000', date '2015-05-04'),
  ('val1c', 12S, 16, 219L, float(17), 25D, 26E2, timestamp '2016-05-04 01:01:00.000', date '2016-05-04'),
  ('val1b', null, 16, 319L, float(17), 25D, 26E2, timestamp '2017-05-04 01:01:00.000', null),
  ('val2e', 8S, null, 419L, float(17), 25D, 26E2, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ('val1f', 19S, null, 519L, float(17), 25D, 26E2, timestamp '2014-05-04 01:01:00.000', date '2014-05-04'),
  ('val1b', 10S, 12, 19L, float(17), 25D, 26E2, timestamp '2014-06-04 01:01:00.000', date '2014-06-04'),
  ('val1b', 8S, 16, 19L, float(17), 25D, 26E2, timestamp '2014-07-04 01:01:00.000', date '2014-07-04'),
  ('val1c', 12S, 16, 19L, float(17), 25D, 26E2, timestamp '2014-08-04 01:01:00.000', date '2014-08-05'),
  ('val1e', 8S, null, 19L, float(17), 25D, 26E2, timestamp '2014-09-04 01:01:00.000', date '2014-09-04'),
  ('val1f', 19S, null, 19L, float(17), 25D, 26E2, timestamp '2014-10-04 01:01:00.000', date '2014-10-04'),
  ('val1b', null, 16, 19L, float(17), 25D, 26E2, timestamp '2014-05-04 01:01:00.000', null)
  as t2(t2a, t2b, t2c, t2d, t2e, t2f, t2g, t2h, t2i);

SELECT hex(count_min_sketch(t2d, seed => 1, epsilon => 0.5d, confidence => 0.5d)) FROM t2;
SELECT hex(count_min_sketch(seed => 1, epsilon => 0.5d, confidence => 0.5d, column => t2d)) FROM t2;
SELECT hex(count_min_sketch(t2d, 0.5d, seed => 1, confidence => 0.5d)) FROM t2;

-- Test for tabled value functions explode and explode_outer
SELECT * FROM explode(collection => array(1, 2));
SELECT * FROM explode_outer(collection => map('a', 1, 'b', 2));
SELECT * FROM explode(array(1, 2)), explode(array(3, 4));
SELECT * FROM explode(array(1, 2)) AS t, LATERAL explode(array(3 * t.col, 4 * t.col));
SELECT num, val, 'Spark' FROM explode(map(1, 'a', 2, 'b')) AS t(num, val);

-- Test for tabled value functions posexplode and posexplode_outer
SELECT * FROM posexplode(collection => array(1, 2));
SELECT * FROM posexplode_outer(collection => map('a', 1, 'b', 2));
SELECT * FROM posexplode(array(1, 2)), posexplode(array(3, 4));
SELECT * FROM posexplode(array(1, 2)) AS t, LATERAL posexplode(array(3 * t.col, 4 * t.col));
SELECT pos, num, val, 'Spark' FROM posexplode(map(1, 'a', 2, 'b')) AS t(pos, num, val);

-- Test for tabled value functions inline and inline_outer
SELECT * FROM inline(input => array(struct(1, 'a'), struct(2, 'b')));
SELECT * FROM inline_outer(input => array(struct(1, 'a'), struct(2, 'b')));
SELECT * FROM inline(array(struct(1, 'a'), struct(2, 'b'))), inline(array(struct(3, 'c'), struct(4, 'd')));
SELECT * FROM inline(array(struct(1, 'a'), struct(2, 'b'))) AS t, LATERAL inline(array(struct(3 * t.col1, 4 * t.col1)));
SELECT num, val, 'Spark' FROM inline(array(struct(1, 'a'), struct(2, 'b'))) AS t(num, val);

-- Test for wrapped EXPLODE call to check error preservation
SELECT * FROM explode(collection => explode(array(1)));
SELECT * FROM explode(collection => explode(collection => array(1)));

-- Test with TABLE parser rule
CREATE OR REPLACE TEMPORARY VIEW v AS SELECT id FROM range(0, 8);
SELECT * FROM explode(collection => TABLE(v));

-- Unexpected positional argument
SELECT mask(lowerChar => 'q', 'AbCD123-@$#', upperChar => 'Q', otherChar => 'o', digitChar => 'd');

-- Duplicate parameter assignment
SELECT mask('AbCD123-@$#', lowerChar => 'q', upperChar => 'Q', otherChar => 'o', digitChar => 'd', digitChar => 'e');
SELECT mask('AbCD123-@$#', lowerChar => 'q', upperChar => 'Q', otherChar => 'o', digitChar => 'd', str => 'AbC');

-- Required parameter not found
SELECT mask(lowerChar => 'q', upperChar => 'Q', otherChar => 'o', digitChar => 'd');

-- Unrecognized parameter name
SELECT mask('AbCD123-@$#', lowerChar => 'q', upperChar => 'Q', otherChar => 'o', digitChar => 'd', cellular => 'automata');

-- Named arguments not supported
SELECT encode(str => 'a', charset => 'utf-8');

-- Wrong number of arguments
SELECT mask('AbCD123-@$#', 'Q', 'q', 'd', 'o', 'k');
