-- test cases for ilike any

CREATE OR REPLACE TEMPORARY VIEW ilike_any_table AS SELECT * FROM (VALUES
  ('Google', '%Oo%'),
  ('FaceBook', '%oO%'),
  ('linkedIn', '%IN'))
  as t1(company, pat);

SELECT company FROM ilike_any_table WHERE company ILIKE ANY ('%oo%', '%IN', 'fA%');

SELECT company FROM ilike_any_table WHERE company ILIKE ANY ('microsoft', '%yoo%');

select
    company,
    CASE
        WHEN company ILIKE ANY ('%oO%', '%IN', 'Fa%') THEN 'Y'
        ELSE 'N'
    END AS is_available,
    CASE
        WHEN company ILIKE ANY ('%OO%', 'fa%') OR company ILIKE ANY ('%in', 'MS%') THEN 'Y'
        ELSE 'N'
    END AS mix
FROM ilike_any_table;

-- Mix test with constant pattern and column value
SELECT company FROM ilike_any_table WHERE company ILIKE ANY ('%zZ%', pat);

-- not ilike any test
SELECT company FROM ilike_any_table WHERE company NOT ILIKE ANY ('%oO%', '%iN', 'fa%');
SELECT company FROM ilike_any_table WHERE company NOT ILIKE ANY ('microsoft', '%yOo%');
SELECT company FROM ilike_any_table WHERE company NOT ILIKE ANY ('%oo%', 'Fa%');
SELECT company FROM ilike_any_table WHERE NOT company ILIKE ANY ('%OO%', 'fa%');

-- null test
SELECT company FROM ilike_any_table WHERE company ILIKE ANY ('%oO%', NULL);
SELECT company FROM ilike_any_table WHERE company NOT ILIKE ANY ('%oo%', NULL);
SELECT company FROM ilike_any_table WHERE company ILIKE ANY (NULL, NULL);
SELECT company FROM ilike_any_table WHERE company NOT ILIKE ANY (NULL, NULL);

-- negative case
SELECT company FROM ilike_any_table WHERE company ILIKE ANY ();
