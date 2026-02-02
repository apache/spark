-- from_csv
select from_csv('1, 3.14', 'a INT, f FLOAT');
select from_csv('26/08/2015', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));
-- Check if errors handled
select from_csv('1', 1);
select from_csv('1', 'a InvalidType');
select from_csv('1', 'Array<int>');
select from_csv('1', 'a INT', named_struct('mode', 'PERMISSIVE'));
select from_csv('1', 'a INT', map('mode', 1));
select from_csv();
-- infer schema of json literal
select from_csv('1,abc', schema_of_csv('1,abc'));
select schema_of_csv('1|abc', map('delimiter', '|'));
select schema_of_csv(null);
select schema_of_csv(42);
CREATE TEMPORARY VIEW csvTable(csvField, a) AS SELECT * FROM VALUES ('1,abc', 'a');
SELECT schema_of_csv(csvField) FROM csvTable;
-- Clean up
DROP VIEW IF EXISTS csvTable;
-- to_csv
select to_csv(named_struct('a', 1, 'b', 2));
select to_csv(named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')), map('timestampFormat', 'dd/MM/yyyy'));
-- Check if errors handled
select to_csv(named_struct('a', 1, 'b', 2), named_struct('mode', 'PERMISSIVE'));
select to_csv(named_struct('a', 1, 'b', 2), map('mode', 1));

-- TIME type tests
select from_csv('14:30:45', 'time TIME(0)');
select from_csv('14:30:45.123', 'time TIME(3)');
select from_csv('14:30:45.123456', 'time TIME(6)');
select from_csv('14-30-45.123456', 'time TIME(6)', map('timeFormat', 'HH-mm-ss.SSSSSS'));

select to_csv(named_struct('time', TIME'14:30:45'));
select to_csv(named_struct('time', TIME'14:30:45.123456'));
select to_csv(named_struct('time', TIME'14:30:45.123456'), map('timeFormat', 'HH-mm-ss.SSSSSS'));

select from_csv(to_csv(named_struct('time', TIME'14:30:45')), 'time TIME(0)');
select from_csv(to_csv(named_struct('time', TIME'14:30:45.1')), 'time TIME(1)');
select from_csv(to_csv(named_struct('time', TIME'14:30:45.12')), 'time TIME(2)');
select from_csv(to_csv(named_struct('time', TIME'14:30:45.123')), 'time TIME(3)');
select from_csv(to_csv(named_struct('time', TIME'14:30:45.1234')), 'time TIME(4)');
select from_csv(to_csv(named_struct('time', TIME'14:30:45.12345')), 'time TIME(5)');
select from_csv(to_csv(named_struct('time', TIME'14:30:45.123456')), 'time TIME(6)');

select from_csv('25:00:00', 'time TIME');
select from_csv('invalid', 'time TIME');
select from_csv('', 'time TIME');
select from_csv(null, 'time TIME');

-- Schema inference: TIME type is never auto-inferred (requires explicit schema).
-- Time-like strings are inferred as TIMESTAMP by CSV's existing inference logic.
select schema_of_csv('14:30:45');
select schema_of_csv('14:30:45.123456');
