DROP TABLE tmp_jo_tab_test;
CREATE table tmp_jo_tab_test (message_line STRING)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/input.txt'
OVERWRITE INTO TABLE tmp_jo_tab_test;

select size(split(message_line, '\t')) from tmp_jo_tab_test;
