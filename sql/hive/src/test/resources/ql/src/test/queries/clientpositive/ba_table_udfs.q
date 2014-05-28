USE default;

CREATE TABLE dest1(bytes1 BINARY,
                   bytes2 BINARY,
                   string STRING);

FROM src INSERT OVERWRITE TABLE dest1
SELECT
  CAST(key AS BINARY),
  CAST(value AS BINARY),
  value
ORDER BY value
LIMIT 100;

--Add in a null row for good measure
INSERT INTO TABLE dest1 SELECT NULL, NULL, NULL FROM dest1 LIMIT 1;

-- this query tests all the udfs provided to work with binary types

SELECT
  bytes1,
  bytes2,
  string,
  LENGTH(bytes1),
  CONCAT(bytes1, bytes2),
  SUBSTR(bytes2, 1, 4),
  SUBSTR(bytes2, 3),
  SUBSTR(bytes2, -4, 3),
  HEX(bytes1),
  UNHEX(HEX(bytes1)),
  BASE64(bytes1),
  UNBASE64(BASE64(bytes1)),
  HEX(ENCODE(string, 'US-ASCII')),
  DECODE(ENCODE(string, 'US-ASCII'), 'US-ASCII')
FROM dest1
ORDER BY bytes2;
