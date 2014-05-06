-- escaped column names in distribute by by are not working jira 3267
explain
select key, value from src distribute by key, value;

explain
select `key`, value from src distribute by `key`, value;
