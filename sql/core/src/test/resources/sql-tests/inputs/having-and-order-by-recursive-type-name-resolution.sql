-- This test file contains queries that test recursive types name resolution in ORDER BY and HAVING clauses.

-- Alias type: String, Table column type: Struct
SELECT 'a' AS col1 FROM VALUES (NAMED_STRUCT('a', 1)) t (col1) GROUP BY col1 ORDER BY col1.a;
SELECT 'a' AS col1 FROM VALUES (NAMED_STRUCT('a', 1)) t (col1) GROUP BY col1 HAVING col1.a > 0;
SELECT 'a' AS col1 FROM VALUES (NAMED_STRUCT('a', 1)) t (col1) GROUP BY col1 HAVING col1.a > 0 ORDER BY col1.a;

-- Alias type: Struct, Table column type: String
SELECT NAMED_STRUCT('a', 1) AS col1 FROM VALUES ('a') t (col1) GROUP BY col1 ORDER BY col1.a;
SELECT NAMED_STRUCT('a', 1) AS col1 FROM VALUES ('a') t (col1) GROUP BY col1 HAVING col1.a > 0;
SELECT NAMED_STRUCT('a', 1) AS col1 FROM VALUES ('a') t (col1) GROUP BY col1 HAVING col1.a > 0 ORDER BY col1.a;

-- Alias type: Struct, Table column type: Struct
SELECT NAMED_STRUCT('a', 1) AS col1 FROM VALUES (NAMED_STRUCT('a', 0)) t (col1) GROUP BY col1 ORDER BY col1.a;
SELECT NAMED_STRUCT('a', 1) AS col1 FROM VALUES (NAMED_STRUCT('a', 0)) t (col1) GROUP BY col1 HAVING col1.a > 0;
SELECT NAMED_STRUCT('a', 1) AS col1 FROM VALUES (NAMED_STRUCT('a', 0)) t (col1) GROUP BY col1 HAVING col1.a > 0 ORDER BY col1.a;

-- Alias type: String, Table column type: Array
SELECT 'a' AS col1 FROM VALUES (ARRAY(1)) t (col1) GROUP BY col1 ORDER BY col1[0];
SELECT 'a' AS col1 FROM VALUES (ARRAY(1)) t (col1) GROUP BY col1 HAVING col1[0] > 0;
SELECT 'a' AS col1 FROM VALUES (ARRAY(1)) t (col1) GROUP BY col1 HAVING col1[0] > 0 ORDER BY col1[0];

-- Alias type: Array, Table column type: String
SELECT ARRAY(1) AS col1 FROM VALUES ('a') t (col1) GROUP BY col1 ORDER BY col1[0];
SELECT ARRAY(1) AS col1 FROM VALUES ('a') t (col1) GROUP BY col1 HAVING col1[0] > 0;

-- Alias type: Struct<Struct>, Table column type: String
SELECT NAMED_STRUCT('a', NAMED_STRUCT('b', 1)) AS col1 FROM VALUES ('a') t (col1) GROUP BY col1 ORDER BY col1.a.b;
SELECT NAMED_STRUCT('a', NAMED_STRUCT('b', 1)) AS col1 FROM VALUES ('a') t (col1) GROUP BY col1 HAVING col1.a.b > 0;
SELECT NAMED_STRUCT('a', NAMED_STRUCT('b', 1)) AS col1 FROM VALUES ('a') t (col1) GROUP BY col1 HAVING col1.a.b > 0 ORDER BY col1.a.b;

-- Alias type: Array<Struct>, Table column type: String
SELECT ARRAY(NAMED_STRUCT('a', 1)) AS col1 FROM VALUES ('a') t (col1) GROUP BY col1 ORDER BY col1[0].a;
SELECT ARRAY(NAMED_STRUCT('a', 1)) AS col1 FROM VALUES ('a') t (col1) GROUP BY col1 HAVING col1[0].a > 0;

-- Alias type: String, Table column type: Map
SELECT 'a' AS col1 FROM VALUES (MAP('key', 1)) t (col1) GROUP BY col1 ORDER BY col1['key'];
SELECT 'a' AS col1 FROM VALUES (MAP('key', 1)) t (col1) GROUP BY col1 HAVING col1['key'] > 0;
SELECT 'a' AS col1 FROM VALUES (MAP('key', 1)) t (col1) GROUP BY col1 HAVING col1['key'] > 0 ORDER BY col1['key'];

-- Alias type: Map, Table column type: String
SELECT MAP('key', 1) AS col1 FROM VALUES ('a') t (col1) GROUP BY col1 ORDER BY col1['key'];
SELECT MAP('key', 1) AS col1 FROM VALUES ('a') t (col1) GROUP BY col1 HAVING col1['key'] > 0;

-- Using ORDER BY with named_struct
SELECT named_struct('a',1) as col, col1
FROM values(named_struct('a',1))
ORDER BY col1.a;

-- Using HAVING with named_struct
SELECT named_struct('a', 1) AS col1, col1
FROM values(named_struct('a', 1))
GROUP BY col1
HAVING col1.a > 0;

-- Using array instead of named_struct with ORDER BY
SELECT array(1, 2, 3) AS col1, col1
FROM values(array(1, 2, 3))
ORDER BY col1[0];

-- Using array with HAVING
SELECT array(1, 2, 3) AS col1, col1
FROM values(array(1, 2, 3))
GROUP BY col1
HAVING col1[1] > 1;

-- Using map with ORDER BY
SELECT map('a', 1, 'b', 2) AS col1, col1
FROM values(map('a', 1, 'b', 2))
ORDER BY col1['a'];

-- Using map with HAVING
SELECT map('a', 1, 'b', 2) AS col1, col1
FROM values(map('a', 1, 'b', 2))
GROUP BY col1
HAVING col1['b'] > 1;

-- Both alias and column are named the `col1`, but fields are named `a` and `b`

-- Reference to column field
SELECT NAMED_STRUCT('b', 1) AS col1 FROM VALUES (NAMED_STRUCT('a', 0)) t (col1) GROUP BY col1 ORDER BY col1.a;
SELECT NAMED_STRUCT('b', 1) AS col1 FROM VALUES (NAMED_STRUCT('a', 0)) t (col1) GROUP BY col1 HAVING col1.a > 0;
SELECT NAMED_STRUCT('b', 1) AS col1 FROM VALUES (NAMED_STRUCT('a', 0)) t (col1) GROUP BY col1 HAVING col1.a > 0 ORDER BY col1.a;

-- Reference to alias field
SELECT NAMED_STRUCT('b', 1) AS col1 FROM VALUES (NAMED_STRUCT('a', 0)) t (col1) GROUP BY col1 ORDER BY col1.b;
SELECT NAMED_STRUCT('b', 1) AS col1 FROM VALUES (NAMED_STRUCT('a', 0)) t (col1) GROUP BY col1 HAVING col1.b > 0;
SELECT NAMED_STRUCT('b', 1) AS col1 FROM VALUES (NAMED_STRUCT('a', 0)) t (col1) GROUP BY col1 HAVING col1.b > 0 ORDER BY col1.b;

SELECT col1.name
FROM VALUES (NAMED_STRUCT('name', 'test', 'id', 1)) t (col1)
GROUP BY col1
HAVING col1.id > 0
ORDER BY col1.name;

SELECT col1.items[0]
FROM VALUES (NAMED_STRUCT('items', ARRAY(1, 2, 3))) t (col1)
GROUP BY col1
HAVING col1.items[0] > 0
ORDER BY col1.items[0];

SELECT col1.data['key']
FROM VALUES (NAMED_STRUCT('data', MAP('key', 'value', 'num', '42'))) t (col1)
GROUP BY col1
HAVING col1.data['num'] IS NOT NULL
ORDER BY col1.data['key'];

SELECT col1.users[0].name
FROM VALUES (
             NAMED_STRUCT('users', ARRAY(NAMED_STRUCT('name', 'John', 'age', 30)))
     ) t (col1)
GROUP BY col1
HAVING col1.users[0].age > 25
ORDER BY col1.users[0].name;

SELECT col1.person.address.city
FROM VALUES (
             NAMED_STRUCT('person', NAMED_STRUCT('address', NAMED_STRUCT('city', 'NYC', 'zip', 10001)))
     ) t (col1)
GROUP BY col1
HAVING col1.person.address.zip > 10000
ORDER BY col1.person.address.city;

SELECT col1.matrix[0][1]
FROM VALUES (
             NAMED_STRUCT('matrix', ARRAY(ARRAY(1, 2), ARRAY(3, 4)))
     ) t (col1)
GROUP BY col1
HAVING col1.matrix[0][0] < 5
ORDER BY col1.matrix[0][1];

SELECT col1.data[0].props['status']
FROM VALUES (
             NAMED_STRUCT('data', ARRAY(NAMED_STRUCT('props', MAP('status', 'active'))))
     ) t (col1)
GROUP BY col1
HAVING col1.data[0].props['status'] = 'active';

SELECT col1.nums[0] + col1.nums[1] AS sum_val
FROM VALUES (NAMED_STRUCT('nums', ARRAY(10, 20))) t (col1)
GROUP BY col1
HAVING col1.nums[0] + col1.nums[1] > 25
ORDER BY col1.nums[0];
