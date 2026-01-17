
-- single row, without table and column alias
select * from values ("one", 1);

-- single row, without column alias
select * from values ("one", 1) as data;

-- single row
select * from values ("one", 1) as data(a, b);

-- single column multiple rows
select * from values 1, 2, 3 as data(a);

-- three rows
select * from values ("one", 1), ("two", 2), ("three", null) as data(a, b);

-- null type
select * from values ("one", null), ("two", null) as data(a, b);

-- int and long coercion
select * from values ("one", 1), ("two", 2L) as data(a, b);

-- foldable expressions
select * from values ("one", 1 + 0), ("two", 1 + 3L) as data(a, b);

-- expressions with alias
select * from values ("one", 1 as one) as data(a, b);

-- literal functions
select a from values ("one", current_timestamp) as data(a, b);

-- complex types
select * from values ("one", array(0, 1)), ("two", array(2, 3)) as data(a, b);

-- decimal and double coercion
select * from values ("one", 2.0), ("two", 3.0D) as data(a, b);

-- nondeterministic function rand - should now work
select * from values ("one", rand(5)), ("two", 3.0D) as data(a, b);

-- error reporting: different number of columns
select * from values ("one", 2.0), ("two") as data(a, b);

-- error reporting: types that are incompatible
select * from values ("one", array(0, 1)), ("two", struct(1, 2)) as data(a, b);

-- error reporting: number aliases different from number data values
select * from values ("one"), ("two") as data(a, b);

-- error reporting: unresolved expression
select * from values ("one", random_not_exist_func(1)), ("two", 2) as data(a, b);

-- error reporting: aggregate expression
select * from values ("one", count(1)), ("two", 2) as data(a, b);

-- string to timestamp
select * from values (timestamp('1991-12-06 00:00:00.0'), array(timestamp('1991-12-06 01:00:00.0'), timestamp('1991-12-06 12:00:00.0'))) as data(a, b);

-- ReplaceExpressions as row
select * from values (try_add(5, 0));
select * from values (try_divide(5, 0));
select * from values (10 + try_divide(5, 0));

-- now() should be kept as tempResolved inline expression.
select count(distinct ct) from values now(), now(), now() as data(ct);

-- current_timestamp() should be kept as tempResolved inline expression.
select count(distinct ct) from values current_timestamp(), current_timestamp() as data(ct);

-- nondeterministic expressions with multiple rows
select count(*) from values (rand()), (rand()), (rand()) as data(a);

-- nondeterministic with different seeds produces different sequences
select count(*) from values (rand(1)), (rand(2)), (rand(3)) as data(a);

-- random() function (alias for rand)
select count(*) from values (random()), (random()) as data(a);

-- uuid() function
select length(a) from values (uuid()), (uuid()) as data(a);

-- mix of deterministic and nondeterministic
select a from values (1, rand(5)), (2, rand(5)) as data(a, b);

-- nondeterministic in more complex expressions
select count(*) from values (1 + rand(5)), (2 + rand(5)) as data(a);

-- ============================================================================
-- LATERAL VALUES with correlation (Phase 2)
-- ============================================================================

-- basic correlated LATERAL VALUES
select * from values (1) as t(c1), lateral (values (t.c1)) as s(c2);

-- correlated with multiple columns
select * from values (1, 2) as t(c1, c2), lateral (values (t.c1, t.c2)) as s(c3, c4);

-- correlated with expressions
select * from values (1, 2) as t(c1, c2), lateral (values (t.c1 + t.c2)) as s(c3);

-- multiple rows in outer table
select * from values (1), (2), (3) as t(c1), lateral (values (t.c1 * 2)) as s(c2) order by c1;

-- multiple rows in correlated VALUES
select * from values (1, 2) as t(c1, c2), lateral (values (t.c1), (t.c2)) as s(c3) order by c3;

-- multiple rows in both outer and inner
select * from values (1), (2) as t(c1), lateral (values (t.c1), (t.c1 + 10)) as s(c2) order by c1, c2;

-- correlated with mix of expressions
select * from values (1, 2) as t(c1, c2),
  lateral (values (t.c1, t.c2, t.c1 + t.c2)) as s(c3, c4, c5);

-- ============================================================================
-- Combining non-deterministic with correlation
-- ============================================================================

-- correlation with nondeterministic
select t.c1, s.c2 from values (1) as t(c1),
  lateral (values (t.c1, rand())) as s(c2, c3) order by c1;

-- multiple rows with correlation and nondeterministic
select t.c1 from values (1), (2) as t(c1),
  lateral (values (t.c1, rand())) as s(c2, c3) order by c1;

-- multiple VALUES rows with correlation and nondeterministic
select t.c1 from values (1) as t(c1),
  lateral (values (t.c1, rand()), (t.c1 + 1, rand())) as s(c2, c3) order by c2;

-- ============================================================================
-- Edge cases and complex scenarios
-- ============================================================================

-- correlated with CURRENT_LIKE
select * from values (1) as t(c1),
  lateral (values (t.c1, current_date)) as s(c2, c3);

-- correlated with type coercion
select * from values (1) as t(c1),
  lateral (values (t.c1, 2.0), (t.c1 + 1, 3.0)) as s(c2, c3) order by c2;

-- correlated with null
select * from values (1, null) as t(c1, c2),
  lateral (values (t.c1, t.c2)) as s(c3, c4);

-- correlated with complex expressions
select * from values (1, 2, 3) as t(c1, c2, c3),
  lateral (values (t.c1 * t.c2 + t.c3)) as s(c4);

-- left outer lateral join
select * from values (1), (2) as t(c1)
  left join lateral (values (t.c1 * 10)) as s(c2) on true order by c1;
