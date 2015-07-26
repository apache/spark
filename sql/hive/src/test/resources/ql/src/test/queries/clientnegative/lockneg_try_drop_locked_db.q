set hive.lock.numretries=0;

create database lockneg9;

lock database lockneg9 shared;
show locks database lockneg9;

drop database lockneg9;
