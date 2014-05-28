-- current
explain select src.key from src join src src2;
-- ansi cross join
explain select src.key from src cross join src src2;
-- appending condition is allowed
explain select src.key from src cross join src src2 on src.key=src2.key;
