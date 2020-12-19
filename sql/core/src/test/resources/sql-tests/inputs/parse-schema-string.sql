-- Use keywords as attribute names
select from_csv('1', 'create INT');
select from_csv('1', 'cube INT');
select from_json('{"create":1}', 'create INT');
select from_json('{"cube":1}', 'cube INT');
