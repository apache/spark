-- escaped column names in sort by are not working jira 3267
explain
select key, value from src sort by key, value;

explain
select `key`, value from src sort by `key`, value;
