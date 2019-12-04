set spark.sql.legacy.integralDivide.returnBigint=true;

select 5 div 2;
select 5 div 0;
select 5 div null;
select null div 5;
select cast(51 as decimal(10, 0)) div cast(2 as decimal(2, 0));
select cast(5 as decimal(1, 0)) div cast(0 as decimal(2, 0));
select cast(5 as decimal(1, 0)) div cast(null as decimal(2, 0));
select cast(null as decimal(1, 0)) div cast(5 as decimal(2, 0));

set spark.sql.legacy.integralDivide.returnBigint=false;

select 5 div 2;
select 5 div 0;
select 5 div null;
select null div 5;
select cast(51 as decimal(10, 0)) div cast(2 as decimal(2, 0));
select cast(5 as decimal(1, 0)) div cast(0 as decimal(2, 0));
select cast(5 as decimal(1, 0)) div cast(null as decimal(2, 0));
select cast(null as decimal(1, 0)) div cast(5 as decimal(2, 0));
