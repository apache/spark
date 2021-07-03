-- test cases for like any

CREATE OR REPLACE TEMPORARY VIEW like_any_table AS SELECT * FROM (VALUES
  ('google', '%oo%'),
  ('facebook', '%oo%'),
  ('linkedin', '%in'))
  as t1(company, pat);

SELECT company FROM like_any_table WHERE company LIKE ANY ('%oo%', '%in', 'fa%');

SELECT company FROM like_any_table WHERE company LIKE ANY ('microsoft', '%yoo%');

select
    company,
    CASE
        WHEN company LIKE ANY ('%oo%', '%in', 'fa%') THEN 'Y'
        ELSE 'N'
    END AS is_available,
    CASE
        WHEN company LIKE ANY ('%oo%', 'fa%') OR company LIKE ANY ('%in', 'ms%') THEN 'Y'
        ELSE 'N'
    END AS mix
FROM like_any_table;

-- Mix test with constant pattern and column value
SELECT company FROM like_any_table WHERE company LIKE ANY ('%zz%', pat);

-- not like any test
SELECT company FROM like_any_table WHERE company NOT LIKE ANY ('%oo%', '%in', 'fa%');
SELECT company FROM like_any_table WHERE company NOT LIKE ANY ('microsoft', '%yoo%');
SELECT company FROM like_any_table WHERE company NOT LIKE ANY ('%oo%', 'fa%');
SELECT company FROM like_any_table WHERE NOT company LIKE ANY ('%oo%', 'fa%');

-- null test
SELECT company FROM like_any_table WHERE company LIKE ANY ('%oo%', NULL);
SELECT company FROM like_any_table WHERE company NOT LIKE ANY ('%oo%', NULL);
SELECT company FROM like_any_table WHERE company LIKE ANY (NULL, NULL);
SELECT company FROM like_any_table WHERE company NOT LIKE ANY (NULL, NULL);

-- negative case
SELECT company FROM like_any_table WHERE company LIKE ANY ();
