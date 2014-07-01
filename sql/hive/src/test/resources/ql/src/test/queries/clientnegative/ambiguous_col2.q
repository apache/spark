-- DOT
explain select * from (select a.`[kv].*`, b.`[kv].*` from (select * from src) a join (select * from src1) b on (a.key = b.key)) t;
