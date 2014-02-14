set hive.exec.pre.hooks="org.this.is.a.bad.class";

EXPLAIN
SELECT x.* FROM SRC x LIMIT 20;

SELECT x.* FROM SRC x LIMIT 20;
