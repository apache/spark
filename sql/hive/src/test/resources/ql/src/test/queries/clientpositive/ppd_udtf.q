explain
SELECT value from (
  select explode(array(key, value)) as (value) from (
    select * FROM src WHERE key > 400
  ) A
) B WHERE value < 450;

SELECT value from (
  select explode(array(key, value)) as (value) from (
    select * FROM src WHERE key > 400
  ) A
) B WHERE value < 450;
