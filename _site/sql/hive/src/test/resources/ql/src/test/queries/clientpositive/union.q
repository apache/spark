-- union case: both subqueries are map jobs on same input, followed by filesink

EXPLAIN
FROM (
  FROM src select src.key, src.value WHERE src.key < 100
  UNION ALL
  FROM src SELECT src.* WHERE src.key > 100
) unioninput
INSERT OVERWRITE DIRECTORY 'target/warehouse/union.out' SELECT unioninput.*;

FROM (
  FROM src select src.key, src.value WHERE src.key < 100
  UNION ALL
  FROM src SELECT src.* WHERE src.key > 100
) unioninput
INSERT OVERWRITE DIRECTORY 'target/warehouse/union.out' SELECT unioninput.*;

dfs -cat ${system:test.warehouse.dir}/union.out/*;
