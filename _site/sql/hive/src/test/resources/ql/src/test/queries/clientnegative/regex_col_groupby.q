set hive.support.quoted.identifiers=none;
EXPLAIN
SELECT `..`, count(1) FROM srcpart GROUP BY `..`;
