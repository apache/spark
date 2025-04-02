set hive.fileformat.check = false;
create table kv_fileformat_check_txt (key string, value string) stored as textfile;
load data local inpath '../../data/files/kv1.seq' overwrite into table kv_fileformat_check_txt;

create table kv_fileformat_check_seq (key string, value string) stored as sequencefile;
load data local inpath '../../data/files/kv1.txt' overwrite into table kv_fileformat_check_seq;



