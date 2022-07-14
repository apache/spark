-- test cases for ilike

-- null handling
select null ilike 'a';
select 'a' ilike null;
select null ilike null;

-- simple patterns
select 'a' ilike 'a';
select 'a' ilike 'b';
select 'A' ilike 'a';
select 'a' ilike 'A';
select 'abdef' ilike 'aBdef';
select 'a_%b' ilike 'a\\__b';
select 'addb' ilike 'A_%b';
select 'abC' ilike 'a%';
select 'a\nb' ilike 'a_B';

-- empty input
select '' ilike '';
select 'A' ilike '';
select '' ilike 'a';

-- double-escaping backslash
select ilike('\__', '\\\__');
select ilike('\\\__', '%\\%\%');

-- unicode
select 'a\u20ACA' ilike '_\u20AC_';
select 'A€a' ilike '_€_';
select 'a€AA' ilike '_\u20AC_a';
select 'a\u20ACaz' ilike '_€_Z';
select 'ЀЁЂѺΏỀ' ilike 'ѐёђѻώề';

-- escape char
select 'Addb' ilike 'a%#%b' escape '#';
select 'a_%b' ilike 'a%#%B' escape '#';
select 'Addb' ilike 'A%$%b' escape '$';
select 'a_%b' ilike 'a%+%B' escape '+';
