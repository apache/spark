-- reference rhs of semijoin in select-clause
select b.value from src a left semi join src b on (b.key = a.key and b.key = '100');
