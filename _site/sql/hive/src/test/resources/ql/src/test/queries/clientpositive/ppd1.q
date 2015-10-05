set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

EXPLAIN
SELECT src.key as c3 from src where src.key > '2';

SELECT src.key as c3 from src where src.key > '2';

set hive.ppd.remove.duplicatefilters=true;

EXPLAIN
SELECT src.key as c3 from src where src.key > '2';

SELECT src.key as c3 from src where src.key > '2';
