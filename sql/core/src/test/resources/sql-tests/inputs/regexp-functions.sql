-- regexp_extract
SELECT regexp_extract('1a 2b 14m', '\\d+');
SELECT regexp_extract('1a 2b 14m', '\\d+', 0);
SELECT regexp_extract('1a 2b 14m', '\\d+', 1);
SELECT regexp_extract('1a 2b 14m', '\\d+', 2);
SELECT regexp_extract('1a 2b 14m', '\\d+', -1);
SELECT regexp_extract('1a 2b 14m', '(\\d+)?', 1);
SELECT regexp_extract('a b m', '(\\d+)?', 1);
SELECT regexp_extract('1a 2b 14m', '(\\d+)([a-z]+)');
SELECT regexp_extract('1a 2b 14m', '(\\d+)([a-z]+)', 0);
SELECT regexp_extract('1a 2b 14m', '(\\d+)([a-z]+)', 1);
SELECT regexp_extract('1a 2b 14m', '(\\d+)([a-z]+)', 2);
SELECT regexp_extract('1a 2b 14m', '(\\d+)([a-z]+)', 3);
SELECT regexp_extract('1a 2b 14m', '(\\d+)([a-z]+)', -1);
SELECT regexp_extract('1a 2b 14m', '(\\d+)?([a-z]+)', 1);
SELECT regexp_extract('a b m', '(\\d+)?([a-z]+)', 1);

-- regexp_extract_all
SELECT regexp_extract_all('1a 2b 14m', '\\d+');
SELECT regexp_extract_all('1a 2b 14m', '\\d+', 0);
SELECT regexp_extract_all('1a 2b 14m', '\\d+', 1);
SELECT regexp_extract_all('1a 2b 14m', '\\d+', 2);
SELECT regexp_extract_all('1a 2b 14m', '\\d+', -1);
SELECT regexp_extract_all('1a 2b 14m', '(\\d+)?', 1);
SELECT regexp_extract_all('a 2b 14m', '(\\d+)?', 1);
SELECT regexp_extract_all('1a 2b 14m', '(\\d+)([a-z]+)');
SELECT regexp_extract_all('1a 2b 14m', '(\\d+)([a-z]+)', 0);
SELECT regexp_extract_all('1a 2b 14m', '(\\d+)([a-z]+)', 1);
SELECT regexp_extract_all('1a 2b 14m', '(\\d+)([a-z]+)', 2);
SELECT regexp_extract_all('1a 2b 14m', '(\\d+)([a-z]+)', 3);
SELECT regexp_extract_all('1a 2b 14m', '(\\d+)([a-z]+)', -1);
SELECT regexp_extract_all('1a 2b 14m', '(\\d+)?([a-z]+)', 1);
SELECT regexp_extract_all('a 2b 14m', '(\\d+)?([a-z]+)', 1);

-- regexp_replace
SELECT regexp_replace('healthy, wealthy, and wise', '\\w+thy', 'something');
SELECT regexp_replace('healthy, wealthy, and wise', '\\w+thy', 'something', -2);
SELECT regexp_replace('healthy, wealthy, and wise', '\\w+thy', 'something', 0);
SELECT regexp_replace('healthy, wealthy, and wise', '\\w+thy', 'something', 1);
SELECT regexp_replace('healthy, wealthy, and wise', '\\w+thy', 'something', 2);
SELECT regexp_replace('healthy, wealthy, and wise', '\\w+thy', 'something', 8);
SELECT regexp_replace('healthy, wealthy, and wise', '\\w', 'something', 26);
SELECT regexp_replace('healthy, wealthy, and wise', '\\w', 'something', 27);
SELECT regexp_replace('healthy, wealthy, and wise', '\\w', 'something', 30);
SELECT regexp_replace('healthy, wealthy, and wise', '\\w', 'something', null);

-- regexp_like
SELECT regexp_like('1a 2b 14m', '\\d+b');
SELECT regexp_like('1a 2b 14m', '[a-z]+b');
SELECT regexp('1a 2b 14m', '\\d+b');
SELECT regexp('1a 2b 14m', '[a-z]+b');
SELECT rlike('1a 2b 14m', '\\d+b');
SELECT rlike('1a 2b 14m', '[a-z]+b');

-- regexp_count
SELECT regexp_count('1a 2b 14m', '\\d+');
SELECT regexp_count('1a 2b 14m', 'mmm');
SELECT regexp_count('the fox', 'FOX');
SELECT regexp_count('the fox', '(?i)FOX');
SELECT regexp_count('passwd7 plain A1234 a1234', '(?=[^ ]*[a-z])(?=[^ ]*[0-9])[^ ]+');
SELECT regexp_count(null, 'abc');
SELECT regexp_count('abc', null);

-- regexp_substr
SELECT regexp_substr('1a 2b 14m', '\\d+');
SELECT regexp_substr('1a 2b 14m', '\\d+ ');
SELECT regexp_substr('1a 2b 14m', '\\d+(a|b|m)');
SELECT regexp_substr('1a 2b 14m', '\\d{2}(a|b|m)');
SELECT regexp_substr('1a 2b 14m', '');
SELECT regexp_substr('Spark', null);
SELECT regexp_substr(null, '.*');

-- regexp_instr
SELECT regexp_instr('abc', 'b');
SELECT regexp_instr('abc', 'x');
SELECT regexp_instr('ABC', '(?-i)b');
SELECT regexp_instr('1a 2b 14m', '\\d{2}(a|b|m)');
SELECT regexp_instr('abc', null);
SELECT regexp_instr(null, 'b');
