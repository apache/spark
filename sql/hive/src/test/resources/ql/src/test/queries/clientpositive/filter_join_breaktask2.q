




create table T1(c1 string, c2 string, c3 string, c4 string, c5 string, c6 string, c7 string) 
partitioned by (ds string);

create table T2(c1 string, c2 string, c3 string, c0 string, c4 string, c5 string, c6 string, c7 string, c8 string, c9 string, c10 string, c11 string, c12 string, c13 string, c14 string, c15 string, c16 string, c17 string, c18 string, c19 string, c20 string, c21 string, c22 string, c23 string, c24 string,  c25 string) partitioned by (ds string); 

create table T3 (c0 bigint,  c1 bigint, c2 int) partitioned by (ds string);

create table T4 (c0 bigint, c1 string, c2 string, c3 string, c4 string, c5 string, c6 string, c7 string, c8 string, c9 string, c10 string, c11 string, c12 string, c13 string, c14 string, c15 string, c16 string, c17 string, c18 string, c19 string, c20 string, c21 string, c22 string, c23 string, c24 string, c25 string, c26 string, c27 string, c28 string, c29 string, c30 string, c31 string, c32 string, c33 string, c34 string, c35 string, c36 string, c37 string, c38 string, c39 string, c40 string, c41 string, c42 string, c43 string, c44 string, c45 string, c46 string, c47 string, c48 string, c49 string, c50 string, c51 string, c52 string, c53 string, c54 string, c55 string, c56 string, c57 string, c58 string, c59 string, c60 string, c61 string, c62 string, c63 string, c64 string, c65 string, c66 string, c67 bigint, c68 string, c69 string, c70 bigint, c71 bigint, c72 bigint, c73 string, c74 string, c75 string, c76 string, c77 string, c78 string, c79 string, c80 string, c81 bigint, c82 bigint, c83 bigint) partitioned by (ds string);

insert overwrite table T1 partition (ds='2010-04-17') select '5', '1', '1', '1',  0, 0,4 from src tablesample (1 rows);

insert overwrite table T2 partition(ds='2010-04-17') select '5','name', NULL, '2', 'kavin',NULL, '9', 'c', '8', '0', '0', '7', '1','2', '0', '3','2', NULL, '1', NULL, '3','2','0','0','5','10' from src tablesample (1 rows);

insert overwrite table T3 partition (ds='2010-04-17') select 4,5,0 from src tablesample (1 rows);

insert overwrite table T4 partition(ds='2010-04-17') 
select 4,'1','1','8','4','5','1','0','9','U','2','2', '0','2','1','1','J','C','A','U', '2','s', '2',NULL, NULL, NULL,NULL, NULL, NULL,'1','j', 'S', '6',NULL,'1', '2', 'J', 'g', '1', 'e', '2', '1', '2', 'U', 'P', 'p', '3', '0', '0', '0', '1', '1', '1', '0', '0', '0', '6', '2', 'j',NULL, NULL, NULL,NULL,NULL, NULL, '5',NULL, 'j', 'j', 2, 2, 1, '2', '2', '1', '1', '1', '1', '1', '1', 1, 1, 32,NULL from src limit 1;

select * from T2;
select * from T1;
select * from T3;
select * from T4;

SELECT a.c1 as a_c1, b.c1 b_c1, d.c0 as d_c0
FROM T1 a JOIN T2 b 
       ON (a.c1 = b.c1 AND a.ds='2010-04-17' AND b.ds='2010-04-17')
     JOIN T3 c 
       ON (a.c1 = c.c1 AND a.ds='2010-04-17' AND c.ds='2010-04-17')
     JOIN T4 d 
       ON (c.c0 = d.c0 AND c.ds='2010-04-17' AND d.ds='2010-04-17');





