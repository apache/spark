-- Test for tabled value functions variant_explode and variant_explode_outer
SELECT * FROM variant_explode(input => parse_json('["hello", "world"]'));
SELECT * FROM variant_explode_outer(input => parse_json('{"a": true, "b": 3.14}'));
SELECT * FROM variant_explode(parse_json('["hello", "world"]')), variant_explode(parse_json('{"a": true, "b": 3.14}'));
SELECT * FROM variant_explode(parse_json('{"a": ["hello", "world"], "b": {"x": true, "y": 3.14}}')) AS t, LATERAL variant_explode(t.value);
SELECT num, key, val, 'Spark' FROM variant_explode(parse_json('["hello", "world"]')) AS t(num, key, val);
