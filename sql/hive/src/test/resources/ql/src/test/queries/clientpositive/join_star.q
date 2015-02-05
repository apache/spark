create table fact(m1 int, m2 int, d1 int, d2 int);
create table dim1(f1 int, f2 int);
create table dim2(f3 int, f4 int);
create table dim3(f5 int, f6 int);
create table dim4(f7 int, f8 int);
create table dim5(f9 int, f10 int);
create table dim6(f11 int, f12 int);
create table dim7(f13 int, f14 int);

LOAD DATA LOCAL INPATH '../../data/files/fact-data.txt' INTO TABLE fact;
LOAD DATA LOCAL INPATH '../../data/files/dim-data.txt' INTO TABLE dim1;
LOAD DATA LOCAL INPATH '../../data/files/dim-data.txt' INTO TABLE dim2;
LOAD DATA LOCAL INPATH '../../data/files/dim-data.txt' INTO TABLE dim3;
LOAD DATA LOCAL INPATH '../../data/files/dim-data.txt' INTO TABLE dim4;
LOAD DATA LOCAL INPATH '../../data/files/dim-data.txt' INTO TABLE dim5;
LOAD DATA LOCAL INPATH '../../data/files/dim-data.txt' INTO TABLE dim6;
LOAD DATA LOCAL INPATH '../../data/files/dim-data.txt' INTO TABLE dim7;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=5000;

explain select m1, m2, f2 from fact join dim1 on fact.d1=dim1.f1;
select m1, m2, f2 from fact join dim1 on fact.d1=dim1.f1;

explain select m1, m2, f2, f4 from fact join dim1 on fact.d1=dim1.f1 join dim2 on fact.d2=dim2.f3;
select m1, m2, f2, f4 from fact join dim1 on fact.d1=dim1.f1 join dim2 on fact.d2=dim2.f3;

explain select m1, m2, f2, f4 from fact join dim1 on fact.d1= dim1.f1 join dim2 on dim1.f2 = dim2.f3;
select m1, m2, f2, f4 from fact join dim1 on fact.d1= dim1.f1 join dim2 on dim1.f2 = dim2.f3;

explain select m1, m2, f2, f4 from fact Left outer join dim1 on fact.d1= dim1.f1 Left outer join dim2 on dim1.f2 = dim2.f3;
select m1, m2, f2, f4 from fact Left outer join dim1 on fact.d1= dim1.f1 Left outer join dim2 on dim1.f2 = dim2.f3;

explain Select m1, m2, f2, f4, f6, f8, f10, f12, f14
 from fact 
 Left outer join dim1 on  fact.d1= dim1.f1
 Left outer join dim2 on  dim1.f2 = dim2.f3
 Left outer Join dim3 on  fact.d2= dim3.f5
 Left outer Join dim4 on  dim3.f6= dim4.f7
 Left outer join dim5 on  dim4.f8= dim5.f9
 Left outer Join dim6 on  dim3.f6= dim6.f11
 Left outer Join dim7 on  dim6.f12 = dim7.f13;

Select m1, m2, f2, f4, f6, f8, f10, f12, f14
 from fact 
 Left outer join dim1 on  fact.d1= dim1.f1
 Left outer join dim2 on  dim1.f2 = dim2.f3
 Left outer Join dim3 on  fact.d2= dim3.f5
 Left outer Join dim4 on  dim3.f6= dim4.f7
 Left outer join dim5 on  dim4.f8= dim5.f9
 Left outer Join dim6 on  dim3.f6= dim6.f11
 Left outer Join dim7 on  dim6.f12 = dim7.f13;

