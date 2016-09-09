-- HIVE-4618 hive should accept unicode notation like \uxxxx

CREATE  TABLE k1( a string)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001';
desc formatted k1;
drop table k1;

CREATE  TABLE k1( a string)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001';
desc formatted k1;
drop table k1;

CREATE  TABLE k1( a string)ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';
desc formatted k1;
drop table k1;
