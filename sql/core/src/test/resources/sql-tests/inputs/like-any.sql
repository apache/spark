CREATE OR REPLACE TEMPORARY VIEW like_any_table AS SELECT * FROM (VALUES
  ('google', '%oo%'),
  ('facebook', '%oo%'),
  ('linkedin', '%in'))
  as t1(company, pat);

select company from like_any_table where company like any ('%oo%','%in','fa%') ;

select company from like_any_table where company like any ('microsoft','%yoo%') ;

select
    company,
    CASE
        WHEN company like any ('%oo%','%in','fa%') THEN 'Y'
        ELSE 'N'
    END AS is_available,
    CASE
        WHEN company like any ('%oo%','fa%') OR company like any ('%in','ms%') THEN 'Y'
        ELSE 'N'
    END AS mix
From like_any_table;

--Mix test with constant pattern and column value
select company from like_any_table where company like any ('%zz%',pat) ;

-- not like any test

select company from like_any_table where company not like any ('%oo%','%in','fa%') ;
select company from like_any_table where company not like any ('microsoft','%yoo%') ;

-- null test

select company from like_any_table where company like any ('%oo%',null) ;

select company from like_any_table where company not like any ('%oo%',null) ;

select company from like_any_table where company like any (null,null) ;

select company from like_any_table where company not like any (null,null) ;
