-- Test for subexpression elimination.

--SET spark.sql.optimizer.enableJsonExpressionOptimization=false

--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false

--CONFIG_DIM2 spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM2 spark.sql.codegen.factoryMode=NO_CODEGEN

--CONFIG_DIM3 spark.sql.subexpressionElimination.enabled=true
--CONFIG_DIM3 spark.sql.subexpressionElimination.enabled=false

-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
('{"a":1, "b":"2"}', '[{"a": 1, "b":2}, {"a":2, "b":2}]'), ('{"a":1, "b":"2"}', null), ('{"a":2, "b":"3"}', '[{"a": 3, "b":4}, {"a":4, "b":5}]'), ('{"a":5, "b":"6"}', '[{"a": 6, "b":7}, {"a":8, "b":9}]'), (null, '[{"a": 1, "b":2}, {"a":2, "b":2}]')
AS testData(a, b);

SELECT from_json(a, 'struct<a:int,b:string>').a, from_json(a, 'struct<a:int,b:string>').b, from_json(b, 'array<struct<a:int,b:int>>')[0].a, from_json(b, 'array<struct<a:int,b:int>>')[0].b FROM testData;

SELECT if(from_json(a, 'struct<a:int,b:string>').a > 1, from_json(b, 'array<struct<a:int,b:int>>')[0].a, from_json(b, 'array<struct<a:int,b:int>>')[0].a + 1) FROM testData;

SELECT if(isnull(from_json(a, 'struct<a:int,b:string>').a), from_json(b, 'array<struct<a:int,b:int>>')[0].b + 1, from_json(b, 'array<struct<a:int,b:int>>')[0].b) FROM testData;

SELECT case when from_json(a, 'struct<a:int,b:string>').a > 5 then from_json(a, 'struct<a:int,b:string>').b when from_json(a, 'struct<a:int,b:string>').a > 4 then from_json(a, 'struct<a:int,b:string>').b + 1 else from_json(a, 'struct<a:int,b:string>').b + 2 end FROM testData;

SELECT case when from_json(a, 'struct<a:int,b:string>').a > 5 then from_json(b, 'array<struct<a:int,b:int>>')[0].b when from_json(a, 'struct<a:int,b:string>').a > 4 then from_json(b, 'array<struct<a:int,b:int>>')[0].b + 1 else from_json(b, 'array<struct<a:int,b:int>>')[0].b + 2 end FROM testData;

-- With non-deterministic expressions.
SELECT from_json(a, 'struct<a:int,b:string>').a + random() > 2, from_json(a, 'struct<a:int,b:string>').b, from_json(b, 'array<struct<a:int,b:int>>')[0].a, from_json(b, 'array<struct<a:int,b:int>>')[0].b + + random() > 2 FROM testData;

SELECT if(from_json(a, 'struct<a:int,b:string>').a + random() > 5, from_json(b, 'array<struct<a:int,b:int>>')[0].a, from_json(b, 'array<struct<a:int,b:int>>')[0].a + 1) FROM testData;

SELECT case when from_json(a, 'struct<a:int,b:string>').a > 5 then from_json(a, 'struct<a:int,b:string>').b + random() > 5 when from_json(a, 'struct<a:int,b:string>').a > 4 then from_json(a, 'struct<a:int,b:string>').b + 1 + random() > 2 else from_json(a, 'struct<a:int,b:string>').b + 2 + random() > 5 end FROM testData;

-- Clean up
DROP VIEW IF EXISTS testData;