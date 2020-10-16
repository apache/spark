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