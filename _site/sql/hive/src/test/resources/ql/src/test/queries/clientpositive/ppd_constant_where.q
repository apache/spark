-- Test that the partition pruner does not fail when there is a constant expression in the filter

EXPLAIN SELECT COUNT(*) FROM srcpart WHERE ds = '2008-04-08' and 'a' = 'a';

SELECT COUNT(*) FROM srcpart WHERE ds = '2008-04-08' and 'a' = 'a';
