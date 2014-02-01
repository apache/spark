set hive.mapred.supports.subdirectories=true;

-- Test stored as directories
-- it covers a few cases

-- 1. create a table with stored as directories
CREATE TABLE  if not exists stored_as_dirs_multiple (col1 STRING, col2 int, col3 STRING) 
SKEWED BY (col1, col2) ON (('s1',1), ('s3',3), ('s13',13), ('s78',78))  stored as DIRECTORIES;
describe formatted stored_as_dirs_multiple;

-- 2. turn off stored as directories but table is still a skewed table
alter table stored_as_dirs_multiple not stored as DIRECTORIES;
describe formatted stored_as_dirs_multiple;

-- 3. turn off skewed
alter table stored_as_dirs_multiple not skewed;
describe formatted stored_as_dirs_multiple;

-- 4. alter a table to stored as directories
CREATE TABLE stored_as_dirs_single (key STRING, value STRING);
alter table stored_as_dirs_single SKEWED BY (key) ON ('1','5','6') 
stored as DIRECTORIES;
describe formatted stored_as_dirs_single;

-- 5. turn off skewed should turn off stored as directories too
alter table stored_as_dirs_single not skewed;
describe formatted stored_as_dirs_single;

-- 6. turn on stored as directories again
alter table stored_as_dirs_single SKEWED BY (key) ON ('1','5','6') 
stored as DIRECTORIES;
describe formatted stored_as_dirs_single;

-- 7. create table like
create table stored_as_dirs_single_like like stored_as_dirs_single;
describe formatted stored_as_dirs_single_like;

-- cleanup
drop table stored_as_dirs_single;
drop table stored_as_dirs_multiple;
