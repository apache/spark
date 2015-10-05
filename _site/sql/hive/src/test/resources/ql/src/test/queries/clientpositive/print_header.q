
set hive.cli.print.header=true;

SELECT src1.key as k1, src1.value as v1, 
       src2.key as k2, src2.value as v2 FROM 
  (SELECT * FROM src WHERE src.key < 10) src1 
    JOIN 
  (SELECT * FROM src WHERE src.key < 10) src2
  SORT BY k1, v1, k2, v2
  LIMIT 10;

SELECT src.key, sum(substr(src.value,5)) FROM src GROUP BY src.key LIMIT 10;

use default;
