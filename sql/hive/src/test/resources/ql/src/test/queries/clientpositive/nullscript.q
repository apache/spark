
CREATE TABLE nullscript(KEY STRING, VALUE STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE nullscript;
LOAD DATA LOCAL INPATH '../../data/files/nullfile.txt' INTO TABLE nullscript;
explain
select transform(key) using 'cat' as key1 from nullscript;
select transform(key) using 'cat' as key1 from nullscript;



