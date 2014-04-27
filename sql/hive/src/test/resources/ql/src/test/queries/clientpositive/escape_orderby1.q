-- escaped column names in order by are not working jira 3267
explain
select key, value from src order by key, value;

explain
select `key`, value from src order by `key`, value;
