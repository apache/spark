-- try_to_binary
-- base64 valid
select try_to_binary('', 'base64');
select try_to_binary('  ', 'base64');
select try_to_binary(' ab cd ', 'base64');
select try_to_binary(' ab c=', 'base64');
select try_to_binary(' ab cdef= = ', 'base64');
select try_to_binary(
  concat(' b25lIHR3byB0aHJlZSBmb3VyIGZpdmUgc2l4IHNldmVuIGVpZ2h0IG5pbmUgdGVuIGVsZXZlbiB0',
         'd2VsdmUgdGhpcnRlZW4gZm91cnRlZW4gZml2dGVlbiBzaXh0ZWVuIHNldmVudGVlbiBlaWdodGVl'), 'base64');
-- base64 invalid
select try_to_binary('a', 'base64');
select try_to_binary('a?', 'base64');
select try_to_binary('abcde', 'base64');
select try_to_binary('abcd=', 'base64');
select try_to_binary('a===', 'base64');
select try_to_binary('ab==f', 'base64');
-- utf-8
select try_to_binary(
  '∮ E⋅da = Q,  n → ∞, ∑ f(i) = ∏ g(i), ∀x∈ℝ: ⌈x⌉ = −⌊−x⌋, α ∧ ¬β = ¬(¬α ∨ β)', 'utf-8');
select try_to_binary('大千世界', 'utf8');
select try_to_binary('', 'utf-8');
select try_to_binary('  ', 'utf8');
-- hex valid
select try_to_binary('737472696E67');
select try_to_binary('737472696E67', 'hex');
select try_to_binary('');
select try_to_binary('1', 'hex');
select try_to_binary('FF');
select try_to_binary('123');
select try_to_binary('12345');
-- hex invalid
select try_to_binary('GG');
select try_to_binary('01 AF', 'hex');
-- 'format' parameter can be any foldable string value, not just literal.
select try_to_binary('abc', concat('utf', '-8'));
select try_to_binary(' ab cdef= = ', substr('base64whynot', 0, 6));
select try_to_binary(' ab cdef= = ', replace('HEX0', '0'));
-- 'format' parameter is case insensitive.
select try_to_binary('abc', 'Hex');
-- null inputs lead to null result.
select try_to_binary('abc', null);
select try_to_binary(null, 'utf-8');
select try_to_binary(null, null);
select try_to_binary(null, cast(null as string));
-- invalid format
select try_to_binary('abc', 1);
select try_to_binary('abc', 'invalidFormat');