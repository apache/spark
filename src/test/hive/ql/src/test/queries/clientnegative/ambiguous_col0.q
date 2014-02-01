-- TOK_ALLCOLREF
explain select * from (select * from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
