-- try_to_binary
select try_to_binary('abc');
select try_to_binary('abc', 'utf-8');
select try_to_binary('abc', 'base64');
select try_to_binary('abc', 'hex');
-- 'format' parameter can be any foldable string value, not just literal.
select try_to_binary('abc', concat('utf', '-8'));
-- 'format' parameter is case insensitive.
select try_to_binary('abc', 'Hex');
-- null inputs lead to null result.
select try_to_binary('abc', null);
select try_to_binary(null, 'utf-8');
select try_to_binary(null, null);
select try_to_binary(null, cast(null as string));
-- 'format' parameter must be string type or void type.
select try_to_binary(null, cast(null as int));
select try_to_binary('abc', 1);
-- invalid format
select try_to_binary('abc', 'invalidFormat');
-- invalid string input
select try_to_binary('a!', 'base64');
