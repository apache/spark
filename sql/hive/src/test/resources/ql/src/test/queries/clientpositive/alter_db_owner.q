
create database db_alter_onr;
describe database db_alter_onr;

alter database db_alter_onr set owner user user1;
describe database db_alter_onr;

alter database db_alter_onr set owner role role1;
describe database db_alter_onr;
