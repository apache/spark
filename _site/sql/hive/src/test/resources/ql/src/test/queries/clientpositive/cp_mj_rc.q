create table src_six_columns (k1 string, v1 string, k2 string, v2 string, k3 string, v3 string) stored as rcfile;
insert overwrite table src_six_columns select value, value, key, value, value, value from src;
create table src_two_columns (k1 string, v1 string) stored as rcfile;
insert overwrite table src_two_columns select key, value from src;
SELECT /*+ MAPJOIN(six) */ six.*, two.k1 from src_six_columns six join src_two_columns two on (six.k3=two.k1);

SELECT /*+ MAPJOIN(two) */ two.*, six.k3 from src_six_columns six join src_two_columns two on (six.k3=two.k1);
