DESCRIBE FUNCTION unhex;
DESCRIBE FUNCTION EXTENDED unhex;

-- Good inputs

SELECT
  unhex('4D7953514C'),
  unhex('31323637'),
  unhex('61'),
  unhex('2D34'),
  unhex('')
FROM src limit 1;

-- Bad inputs
SELECT
  unhex('MySQL'),
  unhex('G123'),
  unhex('\0')
FROM src limit 1;
