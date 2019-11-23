set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION round;
DESCRIBE FUNCTION EXTENDED round;

SELECT round(null), round(null, 0), round(125, null), 
round(1.0/0.0, 0), round(power(-1.0,0.5), 0)
FROM src tablesample (1 rows);

SELECT
  round(55555), round(55555, 0), round(55555, 1), round(55555, 2), round(55555, 3),
  round(55555, -1), round(55555, -2), round(55555, -3), round(55555, -4),
  round(55555, -5), round(55555, -6), round(55555, -7), round(55555, -8)
FROM src tablesample (1 rows);

SELECT
  round(125.315), round(125.315, 0),
  round(125.315, 1), round(125.315, 2), round(125.315, 3), round(125.315, 4),
  round(125.315, -1), round(125.315, -2), round(125.315, -3), round(125.315, -4),
  round(-125.315), round(-125.315, 0),
  round(-125.315, 1), round(-125.315, 2), round(-125.315, 3), round(-125.315, 4),
  round(-125.315, -1), round(-125.315, -2), round(-125.315, -3), round(-125.315, -4)
FROM src tablesample (1 rows);

SELECT
  round(3.141592653589793, -15), round(3.141592653589793, -16),
  round(3.141592653589793, -13), round(3.141592653589793, -14),
  round(3.141592653589793, -11), round(3.141592653589793, -12),
  round(3.141592653589793, -9), round(3.141592653589793, -10),
  round(3.141592653589793, -7), round(3.141592653589793, -8),
  round(3.141592653589793, -5), round(3.141592653589793, -6),
  round(3.141592653589793, -3), round(3.141592653589793, -4),
  round(3.141592653589793, -1), round(3.141592653589793, -2),
  round(3.141592653589793, 0), round(3.141592653589793, 1),
  round(3.141592653589793, 2), round(3.141592653589793, 3),
  round(3.141592653589793, 4), round(3.141592653589793, 5),
  round(3.141592653589793, 6), round(3.141592653589793, 7),
  round(3.141592653589793, 8), round(3.141592653589793, 9),
  round(3.141592653589793, 10), round(3.141592653589793, 11),
  round(3.141592653589793, 12), round(3.141592653589793, 13),
  round(3.141592653589793, 13), round(3.141592653589793, 14),
  round(3.141592653589793, 15), round(3.141592653589793, 16)
FROM src tablesample (1 rows);

SELECT round(1809242.3151111344, 9), round(-1809242.3151111344, 9), round(1809242.3151111344BD, 9), round(-1809242.3151111344BD, 9)
FROM src tablesample (1 rows);
