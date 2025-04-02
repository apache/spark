set hivevar:key1=value1;

EXPLAIN SELECT * FROM src where key="${key1}";
EXPLAIN SELECT * FROM src where key="${hivevar:key1}";

set hivevar:a=1;
set hivevar:b=a;
set hivevar:c=${hivevar:${hivevar:b}};
EXPLAIN SELECT * FROM src where key="${hivevar:c}";

set hivevar:a;
set hivevar:b;
set hivevar:c;
set hivevar:key1;

