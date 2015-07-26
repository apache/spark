set hive.lock.numretries=0;

create database lockneg4;

lock database lockneg4 exclusive;
lock database lockneg4 shared;
