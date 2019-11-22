-- binary type
select x'00' < x'0f';
select x'00' < x'ff';

-- trim string to numeric
select '1 ' = 1Y;
select '\t1 ' = 1Y;
select '1 ' = 1S;
select '1 ' = 1;
select ' 1' = 1L;
select ' 1' = cast(1.0 as float);
select ' 1.0 ' = 1.0D;
select ' 1.0 ' = 1.0BD;