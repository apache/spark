create table if not exists test_boolean(dummy tinyint);
insert overwrite table test_boolean  select 1 from src tablesample (1 rows);

SELECT 1
FROM (
SELECT TRUE AS flag
FROM test_boolean
) a
WHERE flag;
