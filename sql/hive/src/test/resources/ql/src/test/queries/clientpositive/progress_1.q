set hive.heartbeat.interval=5; 


CREATE TABLE PROGRESS_1(key int, value string) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/kv6.txt' INTO TABLE PROGRESS_1;

select count(1) from PROGRESS_1 t1 join PROGRESS_1 t2 on t1.key=t2.key;


