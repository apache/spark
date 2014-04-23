set hive.optimize.ppd=true;

EXPLAIN EXTENDED
 FROM 
  src a
 LEFT OUTER JOIN 
  srcpart b 
 ON (a.key = b.key AND b.ds = '2008-04-08')
 SELECT a.key, a.value, b.key, b.value
 WHERE a.key > 10 AND a.key < 20 AND b.key > 15 AND b.key < 25;

 FROM 
  src a
 LEFT OUTER JOIN 
  srcpart b 
 ON (a.key = b.key AND b.ds = '2008-04-08')
 SELECT a.key, a.value, b.key, b.value
 WHERE a.key > 10 AND a.key < 20 AND b.key > 15 AND b.key < 25;

EXPLAIN EXTENDED
 FROM 
  srcpart a
 LEFT OUTER JOIN 
  src b 
 ON (a.key = b.key AND a.ds = '2008-04-08')
 SELECT a.key, a.value, b.key, b.value
 WHERE a.key > 10 AND a.key < 20 AND b.key > 15 AND b.key < 25;

 FROM 
  srcpart a
 LEFT OUTER JOIN 
  src b 
 ON (a.key = b.key AND a.ds = '2008-04-08')
 SELECT a.key, a.value, b.key, b.value
 WHERE a.key > 10 AND a.key < 20 AND b.key > 15 AND b.key < 25;


EXPLAIN EXTENDED
 FROM 
  src a
 LEFT OUTER JOIN 
  srcpart b 
 ON (a.key = b.key)
 SELECT a.key, a.value, b.key, b.value
 WHERE a.key > 10 AND a.key < 20 AND b.key > 15 AND b.key < 25 AND b.ds = '2008-04-08';

 FROM 
  src a
 LEFT OUTER JOIN 
  srcpart b 
 ON (a.key = b.key)
 SELECT a.key, a.value, b.key, b.value
 WHERE a.key > 10 AND a.key < 20 AND b.key > 15 AND b.key < 25 AND b.ds = '2008-04-08';

EXPLAIN EXTENDED
 FROM 
  srcpart a
 LEFT OUTER JOIN 
  src b 
 ON (a.key = b.key)
 SELECT a.key, a.value, b.key, b.value
 WHERE a.key > 10 AND a.key < 20 AND b.key > 15 AND b.key < 25 AND a.ds = '2008-04-08';

 FROM 
  srcpart a
 LEFT OUTER JOIN 
  src b 
 ON (a.key = b.key)
 SELECT a.key, a.value, b.key, b.value
 WHERE a.key > 10 AND a.key < 20 AND b.key > 15 AND b.key < 25 AND a.ds = '2008-04-08';

