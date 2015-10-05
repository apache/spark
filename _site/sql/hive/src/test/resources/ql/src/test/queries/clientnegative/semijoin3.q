-- rhs table reference in group by
select * from src a left semi join src b on a.key = b.key group by b.value;
