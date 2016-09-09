set hive.optimize.ppd=true;
-- complex predicates in the where clause

explain extended select a.* from srcpart a where rand(1) < 0.1 and a.ds = '2008-04-08' and not(key > 50 or key < 10) and a.hr like '%2';
select a.* from srcpart a where rand(1) < 0.1 and a.ds = '2008-04-08' and not(key > 50 or key < 10) and a.hr like '%2';

-- without rand for comparison
explain extended select a.* from srcpart a where a.ds = '2008-04-08' and not(key > 50 or key < 10) and a.hr like '%2';
select a.* from srcpart a where a.ds = '2008-04-08' and not(key > 50 or key < 10) and a.hr like '%2';
