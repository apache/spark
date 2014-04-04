EXPLAIN
SELECT * FROM srcpart;

EXPLAIN
SELECT `..` FROM srcpart;

EXPLAIN
SELECT srcpart.`..` FROM srcpart;

EXPLAIN
SELECT `..` FROM srcpart a JOIN srcpart b
ON a.key = b.key AND a.value = b.value;

EXPLAIN
SELECT b.`..` FROM srcpart a JOIN srcpart b
ON a.key = b.key AND a.hr = b.hr AND a.ds = b.ds AND a.key = 103
ORDER BY ds, hr;

SELECT b.`..` FROM srcpart a JOIN srcpart b
ON a.key = b.key AND a.hr = b.hr AND a.ds = b.ds AND a.key = 103
ORDER BY ds, hr;

EXPLAIN
SELECT `.e.` FROM srcpart;

EXPLAIN
SELECT `d.*` FROM srcpart;

EXPLAIN
SELECT `(ds)?+.+` FROM srcpart;

EXPLAIN
SELECT `(ds|hr)?+.+` FROM srcpart ORDER BY key, value LIMIT 10;

SELECT `(ds|hr)?+.+` FROM srcpart ORDER BY key, value LIMIT 10;
