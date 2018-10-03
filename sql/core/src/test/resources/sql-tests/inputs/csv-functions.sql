-- to_csv
select to_csv(named_struct('a', 1, 'b', 2));
select to_csv(named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')), map('timestampFormat', 'dd/MM/yyyy'));

-- Check if errors handled
select to_csv(named_struct('a', 1, 'b', 2), named_struct('mode', 'PERMISSIVE'));
select to_csv(named_struct('a', 1, 'b', 2), map('mode', 1));
select to_csv();
