CREATE OR REPLACE TEMPORARY VIEW like_all_table AS SELECT * FROM (VALUES
  ('google', '%oo%'),
  ('facebook', '%oo%'),
  ('linkedin', '%in'))
  as t1(company, pat);

select company from like_all_table where company like all ('%oo%', '%go%');

select company from like_all_table where company like all ('microsoft', '%yoo%');

select
    company,
    CASE
        WHEN company like all ('%oo%', '%go%') THEN 'Y'
        ELSE 'N'
    END AS is_available,
    CASE
        WHEN company like all ('%oo%', 'go%') OR company like all ('%in', 'ms%') THEN 'Y'
        ELSE 'N'
    END AS mix
From like_all_table ;

-- Mix test with constant pattern and column value
select company from like_all_table where company like all ('%oo%', pat);

-- not like all test

select company from like_all_table where company not like all ('%oo%', '%in', 'fa%');
select company from like_all_table where company not like all ('microsoft', '%yoo%');

-- null test

select company from like_all_table where company like all ('%oo%', null);

select company from like_all_table where company not like all ('%oo%', null);

select company from like_all_table where company not like all (null, null);

select company from like_all_table where company not like all (null, null);

-- negative case
select company from like_any_table where company like all ();
