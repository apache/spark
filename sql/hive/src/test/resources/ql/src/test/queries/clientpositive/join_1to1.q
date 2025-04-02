
CREATE TABLE join_1to1_1(key1 int, key2 int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in5.txt' INTO TABLE join_1to1_1;

CREATE TABLE join_1to1_2(key1 int, key2 int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in6.txt' INTO TABLE join_1to1_2;


set hive.outerjoin.supports.filters=false;

set hive.join.emit.interval=5;

SELECT * FROM join_1to1_1 a join join_1to1_2 b on a.key1 = b.key1 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.value = 66 and b.value = 66 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 and a.value = 66 and b.value = 66 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;

set hive.join.emit.interval=2;
SELECT * FROM join_1to1_1 a join join_1to1_2 b on a.key1 = b.key1 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.value = 66 and b.value = 66 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 and a.value = 66 and b.value = 66 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;

set hive.join.emit.interval=1;
SELECT * FROM join_1to1_1 a join join_1to1_2 b on a.key1 = b.key1 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.value = 66 and b.value = 66 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 and a.value = 66 and b.value = 66 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;



set hive.outerjoin.supports.filters=true;

set hive.join.emit.interval=5;

SELECT * FROM join_1to1_1 a join join_1to1_2 b on a.key1 = b.key1 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.value = 66 and b.value = 66 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 and a.value = 66 and b.value = 66 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;

set hive.join.emit.interval=2;
SELECT * FROM join_1to1_1 a join join_1to1_2 b on a.key1 = b.key1 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.value = 66 and b.value = 66 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 and a.value = 66 and b.value = 66 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;

set hive.join.emit.interval=1;
SELECT * FROM join_1to1_1 a join join_1to1_2 b on a.key1 = b.key1 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.value = 66 and b.value = 66 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;
SELECT * FROM join_1to1_1 a full outer join join_1to1_2 b on a.key1 = b.key1 and a.key2 = b.key2 and a.value = 66 and b.value = 66 ORDER BY a.key1 ASC, a.key2 ASC, a.value ASC, b.key1 ASC, b.key2 ASC, b.value ASC;

