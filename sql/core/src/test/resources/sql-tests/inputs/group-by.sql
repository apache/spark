-- Temporary data.
create temporary view myview as values 128, 256 as v(int_col);

-- group by should produce all input rows,
select int_col, count(*) from myview group by int_col;

-- group by should produce a single row.
select 'foo', count(*) from myview group by 1;

-- group-by should not produce any rows.
select 'foo' from myview where int_col == 0 group by 1;
