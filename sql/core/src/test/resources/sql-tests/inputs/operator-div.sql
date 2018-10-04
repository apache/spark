set spark.sql.legacy.integralDivide.returnBigint=true;

select 5 div 2;
select 5 div 0;
select 5 div null;
select null div 5;

set spark.sql.legacy.integralDivide.returnBigint=false;

select 5 div 2;
select 5 div 0;
select 5 div null;
select null div 5;

