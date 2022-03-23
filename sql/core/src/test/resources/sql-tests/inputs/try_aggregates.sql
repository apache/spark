-- try_sum
SELECT try_sum(col) FROM VALUES (5), (10), (15) AS tab(col);
SELECT try_sum(col) FROM VALUES (5.0), (10.0), (15.0) AS tab(col);
SELECT try_sum(col) FROM VALUES (NULL), (10), (15) AS tab(col);
SELECT try_sum(col) FROM VALUES (NULL), (NULL) AS tab(col);
SELECT try_sum(col) FROM VALUES (9223372036854775807L), (1L) AS tab(col);
-- test overflow in Decimal(38, 0)
SELECT try_sum(col) FROM VALUES (98765432109876543210987654321098765432BD), (98765432109876543210987654321098765432BD) AS tab(col);

SELECT try_sum(col) FROM VALUES (interval '1 months'), (interval '1 months') AS tab(col);
SELECT try_sum(col) FROM VALUES (interval '2147483647 months'), (interval '1 months') AS tab(col);
SELECT try_sum(col) FROM VALUES (interval '1 seconds'), (interval '1 seconds') AS tab(col);
SELECT try_sum(col) FROM VALUES (interval '106751991 DAYS'), (interval '1 DAYS') AS tab(col);
