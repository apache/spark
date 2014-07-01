set hive.mapred.supports.subdirectories=true;

create table original (key STRING, value STRING); 

describe formatted original;

alter table original SKEWED BY (key) ON (1,5,6);

describe formatted original;

drop table original;

create table original2 (key STRING, value STRING) ; 

describe formatted original2;

alter table original2 SKEWED BY (key, value) ON ((1,1),(5,6));

describe formatted original2;

drop table original2;

create table original3 (key STRING, value STRING) SKEWED BY (key, value) ON ((1,1),(5,6)); 

describe formatted original3;

alter table original3 not skewed;

describe formatted original3;

drop table original3;

