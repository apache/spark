-- Argument number exception
select concat_ws();
select format_string();

-- A pipe operator for string concatenation
select 'a' || 'b' || 'c';

-- replace function
select replace('abc', 'b', '123');
select replace('abc', 'b');

-- uuid
select length(uuid()), (uuid() <> uuid());

-- position
select position('bar' in 'foobarbar'), position(null, 'foobarbar'), position('aaads', null);

-- left && right
select left("abcd", 2), left("abcd", 5), left("abcd", '2'), left("abcd", null);
select left(null, -2), left("abcd", -2), left("abcd", 0), left("abcd", 'a');
select right("abcd", 2), right("abcd", 5), right("abcd", '2'), right("abcd", null);
select right(null, -2), right("abcd", -2), right("abcd", 0), right("abcd", 'a');

-- split function
SELECT split('aa1cc2ee3', '[1-9]+');
SELECT split('aa1cc2ee3', '[1-9]+', 2);

-- substring function
SELECT substr('Spark SQL', 5);
SELECT substr('Spark SQL', -3);
SELECT substr('Spark SQL', 5, 1);
SELECT substr('Spark SQL' from 5);
SELECT substr('Spark SQL' from -3);
SELECT substr('Spark SQL' from 5 for 1);
SELECT substring('Spark SQL', 5);
SELECT substring('Spark SQL', -3);
SELECT substring('Spark SQL', 5, 1);
SELECT substring('Spark SQL' from 5);
SELECT substring('Spark SQL' from -3);
SELECT substring('Spark SQL' from 5 for 1);

-- trim/ltrim/rtrim
SELECT trim('yxTomxx', 'xyz'), trim(BOTH 'xyz' FROM 'yxTomxx');
SELECT trim('xxxbarxxx', 'x'), trim(BOTH 'x' FROM 'xxxbarxxx');
SELECT ltrim('zzzytest', 'xyz'), trim(LEADING 'xyz' FROM 'zzzytest');
SELECT ltrim('zzzytestxyz', 'xyz'), trim(LEADING 'xyz' FROM 'zzzytestxyz');
SELECT ltrim('xyxXxyLAST WORD', 'xy'), trim(LEADING 'xy' FROM 'xyxXxyLAST WORD');
SELECT rtrim('testxxzx', 'xyz'), trim(TRAILING 'xyz' FROM 'testxxzx');
SELECT rtrim('xyztestxxzx', 'xyz'), trim(TRAILING 'xyz' FROM 'xyztestxxzx');
SELECT rtrim('TURNERyxXxy', 'xy'), trim(TRAILING 'xy' FROM 'TURNERyxXxy');
