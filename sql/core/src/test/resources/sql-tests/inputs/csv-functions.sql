-- from_csv
select from_csv('1, 3.14', 'a INT, f FLOAT');
select from_csv('26/08/2015', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));
-- Check if errors handled
select from_csv('1', 1);
select from_csv('1', 'a InvalidType');
select from_csv('1', 'a INT', named_struct('mode', 'PERMISSIVE'));
select from_csv('1', 'a INT', map('mode', 1));
select from_csv();
