SET hive.vectorized.execution.enabled = true;

-- TODO: add more stuff here after HIVE-5918 is fixed, such as cbigint and constants
explain
select cdouble / 0.0 from alltypesorc limit 100;
select cdouble / 0.0 from alltypesorc limit 100;

-- There are no zeros in the table, but there is 988888, so use it as zero

-- TODO: add more stuff here after HIVE-5918 is fixed, such as cbigint and constants as numerators
explain
select (cbigint - 988888L) as s1, cdouble / (cbigint - 988888L) as s2, 1.2 / (cbigint - 988888L) 
from alltypesorc where cbigint > 0 and cbigint < 100000000 order by s1, s2 limit 100;
select (cbigint - 988888L) as s1, cdouble / (cbigint - 988888L) as s2, 1.2 / (cbigint - 988888L) 
from alltypesorc where cbigint > 0 and cbigint < 100000000 order by s1, s2 limit 100;

-- There are no zeros in the table, but there is -200.0, so use it as zero

explain
select (cdouble + 200.0) as s1, cbigint / (cdouble + 200.0) as s2, (cdouble + 200.0) / (cdouble + 200.0), cbigint / (cdouble + 200.0), 1 / (cdouble + 200.0), 1.2 / (cdouble + 200.0) 
from alltypesorc where cdouble >= -500 and cdouble < -199 order by s1, s2 limit 100;
select (cdouble + 200.0) as s1, cbigint / (cdouble + 200.0) as s2, (cdouble + 200.0) / (cdouble + 200.0), cbigint / (cdouble + 200.0), 1 / (cdouble + 200.0), 1.2 / (cdouble + 200.0) 
from alltypesorc where cdouble >= -500 and cdouble < -199 order by s1, s2 limit 100;

