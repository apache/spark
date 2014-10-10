FROM src SELECT 'a' LIKE '%a%', 'b' LIKE '%a%', 'ab' LIKE '%a%', 'ab' LIKE '%a_',
  '%_' LIKE '\%\_', 'ab' LIKE '\%\_', 'ab' LIKE '_a%', 'ab' LIKE 'a',
  '' RLIKE '.*', 'a' RLIKE '[ab]', '' RLIKE '[ab]', 'hadoop' RLIKE '[a-z]*', 'hadoop' RLIKE 'o*',
  REGEXP_REPLACE('abc', 'b', 'c'), REGEXP_REPLACE('abc', 'z', 'a'), REGEXP_REPLACE('abbbb', 'bb', 'b'), REGEXP_REPLACE('hadoop', '(.)[a-z]*', '$1ive')
  WHERE src.key = 86
