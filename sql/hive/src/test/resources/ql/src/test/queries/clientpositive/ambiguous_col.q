-- TOK_ALLCOLREF
explain select * from (select a.key, a.* from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
-- DOT
explain select * from (select a.key, a.`[k].*` from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
-- EXPRESSION
explain select * from (select a.key, a.key from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
