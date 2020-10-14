--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=true
--CONFIG_DIM1 spark.sql.optimizeNullAwareAntiJoin=false

create temporary view tab_a as select * from values (1, 1) as tab_a(a1, b1);
create temporary view tab_b as select * from values (1, 1) as tab_b(a2, b2);
create temporary view struct_tab as select struct(col1 as a, col2 as b) as record from
 values (1, 1), (1, 2), (2, 1), (2, 2);

select 1 from tab_a where (a1, b1) not in (select a2, b2 from tab_b);
-- Invalid query, see SPARK-24341
select 1 from tab_a where (a1, b1) not in (select (a2, b2) from tab_b);

-- Aliasing is needed as a workaround for SPARK-24443
select count(*) from struct_tab where record in
  (select (a2 as a, b2 as b) from tab_b);
select count(*) from struct_tab where record not in
  (select (a2 as a, b2 as b) from tab_b);
