


CREATE TABLE tmp_pyang_lv (inputs string) STORED AS RCFILE;
INSERT OVERWRITE TABLE tmp_pyang_lv SELECT key FROM src;

EXPLAIN SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol SORT BY key ASC, myCol ASC LIMIT 1;
EXPLAIN SELECT myTable.* FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LIMIT 3;
EXPLAIN SELECT myTable.myCol, myTable2.myCol2 FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LATERAL VIEW explode(array('a', 'b', 'c')) myTable2 AS myCol2 LIMIT 9;
EXPLAIN SELECT myTable2.* FROM src LATERAL VIEW explode(array(array(1,2,3))) myTable AS myCol LATERAL VIEW explode(myTable.myCol) myTable2 AS myCol2 LIMIT 3;

-- Verify that * selects columns from both tables
SELECT * FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol SORT BY key ASC, myCol ASC LIMIT 1;
-- TABLE.* should be supported
SELECT myTable.* FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LIMIT 3;
-- Multiple lateral views should result in a Cartesian product
SELECT myTable.myCol, myTable2.myCol2 FROM src LATERAL VIEW explode(array(1,2,3)) myTable AS myCol LATERAL VIEW explode(array('a', 'b', 'c')) myTable2 AS myCol2 LIMIT 9;
-- Should be able to reference tables generated earlier
SELECT myTable2.* FROM src LATERAL VIEW explode(array(array(1,2,3))) myTable AS myCol LATERAL VIEW explode(myTable.myCol) myTable2 AS myCol2 LIMIT 3;

EXPLAIN
SELECT myCol from tmp_pyang_lv LATERAL VIEW explode(array(1,2,3)) myTab as myCol limit 3;

SELECT myCol from tmp_PYANG_lv LATERAL VIEW explode(array(1,2,3)) myTab as myCol limit 3;

CREATE TABLE tmp_pyang_src_rcfile (key string, value array<string>) STORED AS RCFILE;
INSERT OVERWRITE TABLE tmp_pyang_src_rcfile SELECT key, array(value) FROM src ORDER BY key LIMIT 20;

SELECT key,value from tmp_pyang_src_rcfile LATERAL VIEW explode(value) myTable AS myCol;
SELECT myCol from tmp_pyang_src_rcfile LATERAL VIEW explode(value) myTable AS myCol;
SELECT * from tmp_pyang_src_rcfile LATERAL VIEW explode(value) myTable AS myCol;

SELECT subq.key,subq.value 
FROM (
SELECT * from tmp_pyang_src_rcfile LATERAL VIEW explode(value) myTable AS myCol
)subq;

SELECT subq.myCol
FROM (
SELECT * from tmp_pyang_src_rcfile LATERAL VIEW explode(value) myTable AS myCol
)subq;

SELECT subq.key 
FROM (
SELECT key, value from tmp_pyang_src_rcfile LATERAL VIEW explode(value) myTable AS myCol
)subq;

EXPLAIN SELECT value, myCol from (SELECT key, array(value[0]) AS value FROM tmp_pyang_src_rcfile GROUP BY value[0], key) a
LATERAL VIEW explode(value) myTable AS myCol;

SELECT value, myCol from (SELECT key, array(value[0]) AS value FROM tmp_pyang_src_rcfile GROUP BY value[0], key) a
LATERAL VIEW explode(value) myTable AS myCol;



