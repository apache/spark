CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
AS testData(a, b);

-- explode function
SELECT explode(array(a, b)) AS elem FROM testData;
SELECT explode(array(a, b)) AS elem, 'Spark' FROM testData;
SELECT explode(array(a, b)) AS null, null FROM testData;
SELECT explode(array(1, 2)) AS null, null, 'column';
SELECT explode(map(1, 'a', 2, 'b'));
SELECT explode(struct(1, 'a', 2, 'b'));
SELECT explode(1);
SELECT explode(null);
SELECT explode(cast (null as array<int>));

-- explode_outer function
SELECT explode_outer(array(a, b)) AS elem FROM testData;
SELECT explode_outer(array(a, b)) AS elem, 'Spark' FROM testData;
SELECT explode_outer(array(a, b)) AS null, null FROM testData;
SELECT explode_outer(array(1, 2)) AS null, null, 'column';
SELECT explode_outer(map(1, 'a', 2, 'b'));
SELECT explode_outer(struct(1, 'a', 2, 'b'));
SELECT explode_outer(1);
SELECT explode_outer(null);
SELECT explode_outer(cast (null as array<int>));

-- posexplode function
SELECT posexplode(array(a, b)) AS elem FROM testData;
SELECT posexplode(array(a, b)) AS (elem, elem2) FROM testData;
SELECT posexplode(array(a, b)) AS (null, null) FROM testData;
SELECT posexplode(array(1, 2)) AS (null, null, column);
SELECT posexplode(map(1, 'a', 2, 'b'));
SELECT posexplode(struct(1, 'a', 2, 'b'));
SELECT posexplode(1);
SELECT posexplode(null);
SELECT posexplode(cast (null as array<int>));

-- posexplode_outer function
SELECT posexplode_outer(array(a, b)) AS elem FROM testData;
SELECT posexplode_outer(array(a, b)) AS (elem, elem2) FROM testData;
SELECT posexplode_outer(array(a, b)) AS (null, null) FROM testData;
SELECT posexplode_outer(array(1, 2)) AS (null, null, column);
SELECT posexplode_outer(map(1, 'a', 2, 'b'));
SELECT posexplode_outer(struct(1, 'a', 2, 'b'));
SELECT posexplode_outer(1);
SELECT posexplode_outer(null);
SELECT posexplode_outer(cast (null as array<int>));

-- inline function
SELECT inline(array(struct(1, 'a')));
SELECT inline(array(struct(null, 'a'), struct(null, 'b')));
SELECT inline(array(struct(a, 'a'), struct(a, 'b'))) FROM testData;
SELECT inline(array(struct(a, 'a'), struct(b, 'b'))) FROM testData;
SELECT inline(1);
SELECT inline(array(1, 'a'));
SELECT inline(struct(1, 'a'));

-- inline_outer function
SELECT inline_outer(array(struct(1, 'a')));
SELECT inline_outer(array(struct(null, 'a'), struct(null, 'b')));
SELECT inline_outer(array(struct(a, 'a'), struct(a, 'b'))) FROM testData;
SELECT inline_outer(array(struct(a, 'a'), struct(b, 'b'))) FROM testData;
SELECT inline_outer(1);
SELECT inline_outer(array(1, 'a'));
SELECT inline_outer(struct(1, 'a'));

-- json_tuple function
select json_tuple('{"a": 1, "b": 2}', 'a', 'b');
select json_tuple();
select json_tuple('{"a": 1}');
select json_tuple('{"a": 1}', 1);
select json_tuple('{"a": 1}', null);

-- stack function
select stack(1, 1, 2, 3);
select stack(2, 1, 1.1, 'a', 2, 2.2, 'b');
select stack(2, 1, 1.1, null, 2, null, 'b');
select stack();
select stack(2, explode(array(1, 2, 3)));
