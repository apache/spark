-- escaped column names in cluster by are not working jira 3267
explain
select key, value from src cluster by key, value;

explain
select `key`, value from src cluster by `key`, value;
