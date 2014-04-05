
CREATE TABLE joinone(key1 int, key2 int, value int);
LOAD DATA LOCAL INPATH '../data/files/in5.txt' INTO TABLE joinone;

CREATE TABLE joinTwo(key1 int, key2 int, value int);
LOAD DATA LOCAL INPATH '../data/files/in6.txt' INTO TABLE joinTwo;

SELECT * FROM joinone JOIN joinTwo ON(joinone.key2=joinTwo.key2) ORDER BY joinone.key1 ASC, joinone.key2 ASC, joinone.value ASC, joinTwo.key1 ASC, joinTwo.key2 ASC, joinTwo.value ASC;
