
set hive.auto.convert.join=true;

CREATE TABLE src11 as SELECT * FROM src;
CREATE TABLE src12 as SELECT * FROM src;
CREATE TABLE src13 as SELECT * FROM src;
CREATE TABLE src14 as SELECT * FROM src;


EXPLAIN SELECT * FROM 
src11 a JOIN
src12 b ON (a.key = b.key) JOIN
(SELECT * FROM (SELECT * FROM src13 UNION ALL SELECT * FROM src14)a )c ON c.value = b.value;