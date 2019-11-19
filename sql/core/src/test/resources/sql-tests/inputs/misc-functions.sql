-- test for misc functions

-- typeof
select typeof(1);
select typeof(1.2);
select typeof(array(1, 2));
select typeof(a) from (values (1), (2), (3.1)) t(a);
