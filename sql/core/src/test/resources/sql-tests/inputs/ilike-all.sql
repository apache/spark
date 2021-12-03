-- test cases for ilike all

CREATE OR REPLACE TEMPORARY VIEW ilike_all_table AS SELECT * FROM (VALUES
  ('gOOgle', '%oo%'),
  ('facebook', '%OO%'),
  ('liNkedin', '%In'))
  as t1(company, pat);

SELECT company FROM ilike_all_table WHERE company ILIKE ALL ('%oO%', '%Go%');

SELECT company FROM ilike_all_table WHERE company ILIKE ALL ('microsoft', '%yoo%');

SELECT 
    company,
    CASE
        WHEN company ILIKE ALL ('%oo%', '%GO%') THEN 'Y'
        ELSE 'N'
    END AS is_available,
    CASE
        WHEN company ILIKE ALL ('%OO%', 'go%') OR company ILIKE ALL ('%IN', 'ms%') THEN 'Y'
        ELSE 'N'
    END AS mix
FROM ilike_all_table ;

-- Mix test with constant pattern and column value
SELECT company FROM ilike_all_table WHERE company ILIKE ALL ('%oo%', pat);

-- not ilike all test
SELECT company FROM ilike_all_table WHERE company NOT ILIKE ALL ('%oo%', '%In', 'Fa%');
SELECT company FROM ilike_all_table WHERE company NOT ILIKE ALL ('microsoft', '%yoo%');
SELECT company FROM ilike_all_table WHERE company NOT ILIKE ALL ('%oo%', 'fA%');
SELECT company FROM ilike_all_table WHERE NOT company ILIKE ALL ('%oO%', 'fa%');

-- null test
SELECT company FROM ilike_all_table WHERE company ILIKE ALL ('%OO%', NULL);
SELECT company FROM ilike_all_table WHERE company NOT ILIKE ALL ('%Oo%', NULL);
SELECT company FROM ilike_all_table WHERE company ILIKE ALL (NULL, NULL);
SELECT company FROM ilike_all_table WHERE company NOT ILIKE ALL (NULL, NULL);

-- negative case
SELECT company FROM ilike_any_table WHERE company ILIKE ALL ();
