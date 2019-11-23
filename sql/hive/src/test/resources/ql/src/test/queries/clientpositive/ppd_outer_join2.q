set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=false;

EXPLAIN
 FROM 
  src a
 RIGHT OUTER JOIN 
  src b 
 ON (a.key = b.key)
 SELECT a.key, a.value, b.key, b.value
 WHERE a.key > '10' AND a.key < '20' AND b.key > '15' AND b.key < '25';

 FROM 
  src a
 RIGHT OUTER JOIN 
  src b 
 ON (a.key = b.key)
 SELECT a.key, a.value, b.key, b.value
 WHERE a.key > '10' AND a.key < '20' AND b.key > '15' AND b.key < '25';

set hive.ppd.remove.duplicatefilters=true;

EXPLAIN
 FROM 
  src a
 RIGHT OUTER JOIN 
  src b 
 ON (a.key = b.key)
 SELECT a.key, a.value, b.key, b.value
 WHERE a.key > '10' AND a.key < '20' AND b.key > '15' AND b.key < '25';

 FROM 
  src a
 RIGHT OUTER JOIN 
  src b 
 ON (a.key = b.key)
 SELECT a.key, a.value, b.key, b.value
 WHERE a.key > '10' AND a.key < '20' AND b.key > '15' AND b.key < '25';
