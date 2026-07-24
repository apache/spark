-- variant_from_arrays

-- Basic object construction; keys are sorted in the resulting variant object.
select cast(variant_from_arrays(array('z', 'a'), array(1, 2)) as string);
-- Empty input produces an empty object.
select cast(variant_from_arrays(cast(array() as array<string>), cast(array() as array<int>)) as string);
-- Null values are kept as variant null.
select cast(variant_from_arrays(array('a', 'b'), array(1, cast(null as int))) as string);
-- Nested values are converted recursively.
select cast(variant_from_arrays(array('a'), array(array(1, 2, 3))) as string);
-- A null array input produces null.
select cast(variant_from_arrays(cast(null as array<string>), array(1)) as string);
-- A null key is rejected.
select variant_from_arrays(array('a', cast(null as string)), array(1, 2));
-- Duplicate keys are rejected.
select variant_from_arrays(array('a', 'a'), array(1, 2));
-- Mismatched array lengths are rejected.
select variant_from_arrays(array('a', 'b'), array(1));
-- A non-string key type is rejected.
select variant_from_arrays(array(1, 2), array('a', 'b'));
-- A value type that cannot be cast to variant is rejected.
select variant_from_arrays(array('a'), array(map(1, 2)));

-- variant_from_entries

-- Basic object construction from key/value struct entries.
select cast(variant_from_entries(array(named_struct('k', 'a', 'v', 1), named_struct('k', 'b', 'v', 2))) as string);
-- Null values are kept as variant null.
select cast(variant_from_entries(array(named_struct('k', 'a', 'v', cast(null as int)))) as string);
-- A null entry makes the whole result null.
select cast(variant_from_entries(array(named_struct('k', 'a', 'v', 1), cast(null as struct<k:string,v:int>))) as string);
-- A null array input produces null.
select cast(variant_from_entries(cast(null as array<struct<k:string,v:int>>)) as string);
-- A null key is rejected.
select variant_from_entries(array(named_struct('k', cast(null as string), 'v', 1)));
-- A non-array-of-pair-struct input is rejected.
select variant_from_entries(array(1, 2));
-- A value type that cannot be cast to variant is rejected.
select variant_from_entries(array(named_struct('k', 'a', 'v', map(1, 2))));
