-- Tests for two-argument IF (SQL standard: IF(cond, value) returns NULL when false)

-- Two-argument IF: returns value when true
SELECT if(1 < 2, 'yes');

-- Two-argument IF: returns NULL when false
SELECT if(1 > 2, 'yes');

-- Two-argument IF: with column reference
SELECT if(id > 1, id * 10) FROM VALUES (1), (2), (3) AS t(id);

-- Three-argument IF still works (regression check)
SELECT if(1 < 2, 'a', 'b');

-- Two-argument IF with complex expression
SELECT if(length('hello') > 3, upper('hello'));

-- Error: one argument
SELECT if(true);

-- Error: four arguments
SELECT if(true, 1, 2, 3);
