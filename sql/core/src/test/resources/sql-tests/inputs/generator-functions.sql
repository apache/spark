-- unnest

-- basic ones
select unnest(array(1,2));
select unnest(array(1, 2, 1.0, '1'));
select unnest(a) from values (array(1, 2)), (array(3, 4, 5)) as v1(a);
select unnest(array(null));

-- null unsupported
select unnest(null);

-- empty set
select unnest(a) from values (array(1, 2)), (array(3, 4)) v1(a) where 1 = 0;

-- explode recursively
select unnest(a) from values (array(array(1, 2), array(3, 4))) v1(a);
select unnest(a) from values (array(array(1, 2), array(3, null))) v1(a);

-- these case will fail in type check
select unnest(a) from values (array(array(1, 2), array(3, array(5, 6)))) v1(a);
select unnest(a) from values (array(array(1, 2), array(3, 4), null)) v1(a);

-- currently we are not able to throw ex for dimensions mismatching in type check
select unnest(a) from values (array(array(1, 2), array(3, 4, 5))) as v1(a);
