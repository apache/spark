-- unnest

select unnest(array(1,2));

-- null unsupported
select unnest(null);

select unnest(array(null));

-- empty set
select unnest(a) from values (array(1, 2)), (array(3, 4)) as v1(a) where 1 = 0;

-- explode recursively
select unnest(a) from values (array(1, 2)), (array(3, 4)) as v1(a);
select unnest(a) from values (array(1, 2)), (array(3, null)) as v1(a);

select unnest(a) from values (array(1, 2)), (array(3, array(5, 6))) as v1(a);

select unnest(array(1, 2, 1.0, '1'));

-- those fails dimensions checking in postgres but escaped from spark
select unnest(a) from values (array(1, 2)), (array(3, 4)), (null) as v1(a);
select unnest(a) from values (array(1, 2)), (array(3, 4, 5)) as v1(a);
