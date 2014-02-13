set hive.auto.convert.join = true;

CREATE TABLE myinput1(key int, value int);
LOAD DATA LOCAL INPATH '../data/files/in1.txt' INTO TABLE myinput1;

SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a JOIN myinput1 b;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a LEFT OUTER JOIN myinput1 b;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a RIGHT OUTER JOIN myinput1 b;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a JOIN myinput1 b ON a.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a JOIN myinput1 b ON a.key = b.key;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a JOIN myinput1 b ON a.value = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a JOIN myinput1 b ON a.value = b.value and a.key=b.key;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.value = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.key;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.key and a.value=b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.key;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.value = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key=b.key and a.value = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.key = b.key;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.value = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.value = b.value and a.key=b.key;

SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1 a LEFT OUTER JOIN myinput1 b ON (a.value=b.value) RIGHT OUTER JOIN myinput1 c ON (b.value=c.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1 a RIGHT OUTER JOIN myinput1 b ON (a.value=b.value) LEFT OUTER JOIN myinput1 c ON (b.value=c.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a LEFT OUTER JOIN myinput1 b RIGHT OUTER JOIN myinput1 c ON a.value = b.value and b.value = c.value;

