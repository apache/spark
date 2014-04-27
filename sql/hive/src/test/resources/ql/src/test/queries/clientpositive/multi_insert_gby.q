--HIVE-3699 Multiple insert overwrite into multiple tables query stores same results in all tables
create table e1 (key string, count int);
create table e2 (key string, count int);

explain FROM src
INSERT OVERWRITE TABLE e1
    SELECT key, COUNT(*) WHERE key>450 GROUP BY key ORDER BY key
INSERT OVERWRITE TABLE e2
    SELECT key, COUNT(*) WHERE key>500 GROUP BY key ORDER BY key;

FROM src
INSERT OVERWRITE TABLE e1
    SELECT key, COUNT(*) WHERE key>450 GROUP BY key ORDER BY key
INSERT OVERWRITE TABLE e2
    SELECT key, COUNT(*) WHERE key>500 GROUP BY key ORDER BY key;

select * from e1;
select * from e2;

explain FROM src
INSERT OVERWRITE TABLE e1
    SELECT key, COUNT(*) WHERE key>450 GROUP BY key ORDER BY key
INSERT OVERWRITE TABLE e2
    SELECT key, COUNT(*) GROUP BY key ORDER BY key;

FROM src
INSERT OVERWRITE TABLE e1
    SELECT key, COUNT(*) WHERE key>450 GROUP BY key ORDER BY key
INSERT OVERWRITE TABLE e2
    SELECT key, COUNT(*) GROUP BY key ORDER BY key;

select * from e1;
select * from e2;
