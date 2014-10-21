-- rhs table reference in the where clause
select a.value from src a left semi join src b on a.key = b.key where b.value = 'val_18';
