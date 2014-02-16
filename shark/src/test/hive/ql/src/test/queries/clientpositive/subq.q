EXPLAIN
FROM (
  FROM src select src.* WHERE src.key < 100
) unioninput
INSERT OVERWRITE DIRECTORY '../build/ql/test/data/warehouse/union.out' SELECT unioninput.*;

FROM (
  FROM src select src.* WHERE src.key < 100
) unioninput
INSERT OVERWRITE DIRECTORY '../build/ql/test/data/warehouse/union.out' SELECT unioninput.*;

dfs -cat ../build/ql/test/data/warehouse/union.out/*;

