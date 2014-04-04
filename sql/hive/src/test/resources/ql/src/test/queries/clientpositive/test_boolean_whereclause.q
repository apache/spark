create table if not exists test_boolean(dummy tinyint);
insert overwrite table test_boolean  select 1 from src limit 1;

SELECT 1
FROM (
SELECT TRUE AS flag
FROM test_boolean
) a
WHERE flag;
