-- TRY_ADD
SELECT try_add(1, 1);
SELECT try_add(2147483647, 1);
SELECT try_add(-2147483648, -1);
SELECT try_add(9223372036854775807L, 1);
SELECT try_add(-9223372036854775808L, -1);

-- TRY_DIVIDE
SELECT try_divide(1, 0.5);
SELECT try_divide(1, 0);
SELECT try_divide(0, 0);