set hive.fetch.task.conversion=more;

EXPLAIN
SELECT
    'abc',        "abc",
    'abc\'',      "abc\"",
    'abc\\',      "abc\\",
    'abc\\\'',    "abc\\\"",
    'abc\\\\',    "abc\\\\",
    'abc\\\\\'',  "abc\\\\\"",
    'abc\\\\\\',  "abc\\\\\\",
    'abc""""\\',  "abc''''\\",
    "awk '{print NR\"\\t\"$0}'",
    'tab\ttab',   "tab\ttab"
FROM src
LIMIT 1;

SELECT
    'abc',        "abc",
    'abc\'',      "abc\"",
    'abc\\',      "abc\\",
    'abc\\\'',    "abc\\\"",
    'abc\\\\',    "abc\\\\",
    'abc\\\\\'',  "abc\\\\\"",
    'abc\\\\\\',  "abc\\\\\\",
    'abc""""\\',  "abc''''\\",
    "awk '{print NR\"\\t\"$0}'",
    'tab\ttab',   "tab\ttab"
FROM src
LIMIT 1;
