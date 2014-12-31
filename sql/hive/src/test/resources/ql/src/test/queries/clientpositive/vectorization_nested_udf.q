SET hive.vectorized.execution.enabled=true;
SELECT SUM(abs(ctinyint)) from alltypesorc;

