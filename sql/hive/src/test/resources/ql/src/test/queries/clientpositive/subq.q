EXPLAIN
FROM (
  FROM src select src.* WHERE src.key < 100
) unioninput
INSERT OVERWRITE DIRECTORY 'target/warehouse/union.out' SELECT unioninput.*;

FROM (
  FROM src select src.* WHERE src.key < 100
) unioninput
INSERT OVERWRITE DIRECTORY 'target/warehouse/union.out' SELECT unioninput.*;

dfs -cat ${system:test.warehouse.dir}/union.out/*;

