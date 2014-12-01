SET hive.vectorized.execution.enabled=true;
SET hive.optimize.index.filter=true;
explain SELECT AVG(cbigint) FROM alltypesorc WHERE cbigint < cdouble;
SELECT AVG(cbigint) FROM alltypesorc WHERE cbigint < cdouble;
