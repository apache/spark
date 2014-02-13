DESCRIBE FUNCTION hex;
DESCRIBE FUNCTION EXTENDED hex;

-- If the argument is a string, hex should return a string containing two hex
-- digits for every character in the input.
SELECT
  hex('Facebook'),
  hex('\0'),
  hex('qwertyuiopasdfghjkl')
FROM src LIMIT 1;

-- If the argument is a number, hex should convert it to hexadecimal.
SELECT
  hex(1),
  hex(0),
  hex(4207849477)
FROM src LIMIT 1;

-- Negative numbers should be treated as two's complement (64 bit).
SELECT hex(-5) FROM src LIMIT 1;
