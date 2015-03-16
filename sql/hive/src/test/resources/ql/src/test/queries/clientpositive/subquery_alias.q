EXPLAIN
FROM (
  FROM src select src.* WHERE src.key < 100
) as unioninput
INSERT OVERWRITE DIRECTORY 'target/warehouse/union.out' SELECT unioninput.*;

EXPLAIN
SELECT * FROM
( SELECT * FROM 
   ( SELECT * FROM src as s ) as src1 
) as src2;

SELECT * FROM
( SELECT * FROM 
   ( SELECT * FROM src as s ) as src1 
) as src2;
