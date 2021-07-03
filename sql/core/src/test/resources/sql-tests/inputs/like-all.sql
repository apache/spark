-- test cases for like all

CREATE OR REPLACE TEMPORARY VIEW like_all_table AS SELECT * FROM (VALUES
  ('google', '%oo%'),
  ('facebook', '%oo%'),
  ('linkedin', '%in'))
  as t1(company, pat);

SELECT company FROM like_all_table WHERE company LIKE ALL ('%oo%', '%go%');

SELECT company FROM like_all_table WHERE company LIKE ALL ('microsoft', '%yoo%');

SELECT 
    company,
    CASE
        WHEN company LIKE ALL ('%oo%', '%go%') THEN 'Y'
        ELSE 'N'
    END AS is_available,
    CASE
        WHEN company LIKE ALL ('%oo%', 'go%') OR company LIKE ALL ('%in', 'ms%') THEN 'Y'
        ELSE 'N'
    END AS mix
FROM like_all_table ;

-- Mix test with constant pattern and column value
SELECT company FROM like_all_table WHERE company LIKE ALL ('%oo%', pat);

-- not like all test
SELECT company FROM like_all_table WHERE company NOT LIKE ALL ('%oo%', '%in', 'fa%');
SELECT company FROM like_all_table WHERE company NOT LIKE ALL ('microsoft', '%yoo%');
SELECT company FROM like_all_table WHERE company NOT LIKE ALL ('%oo%', 'fa%');
SELECT company FROM like_all_table WHERE NOT company LIKE ALL ('%oo%', 'fa%');

-- null test
SELECT company FROM like_all_table WHERE company LIKE ALL ('%oo%', NULL);
SELECT company FROM like_all_table WHERE company NOT LIKE ALL ('%oo%', NULL);
SELECT company FROM like_all_table WHERE company LIKE ALL (NULL, NULL);
SELECT company FROM like_all_table WHERE company NOT LIKE ALL (NULL, NULL);

-- negative case
SELECT company FROM like_any_table WHERE company LIKE ALL ();
