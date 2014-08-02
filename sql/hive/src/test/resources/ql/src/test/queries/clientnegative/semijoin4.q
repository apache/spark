-- rhs table is a view and reference the view in where clause
select a.value from src a left semi join (select key , value from src where key > 100) b on a.key = b.key where b.value = 'val_108' ;

