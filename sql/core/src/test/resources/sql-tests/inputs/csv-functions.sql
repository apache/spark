-- from_csv
select from_csv('1, 3.14', 'a INT, f FLOAT');
select from_csv('26/08/2015', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));
-- Check if errors handled
select from_csv('1', 1);
select from_csv('1', 'a InvalidType');
select from_csv('1', 'a INT', named_struct('mode', 'PERMISSIVE'));
select from_csv('1', 'a INT', map('mode', 1));
select from_csv();
-- infer schema of json literal
select from_csv('1,abc', schema_of_csv('1,abc'));
select schema_of_csv('1|abc', map('delimiter', '|'));
select schema_of_csv(null);
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
