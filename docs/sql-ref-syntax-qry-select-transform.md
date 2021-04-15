---
layout: global
title: TRANSFORM
displayTitle: TRANSFORM
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

### Description

The `TRANSFORM` clause is used to specify a Hive-style transform query specification 
to transform the inputs by running a specified command or script. Users can plug in their own custom 
command or script in the data stream by using `TRANSFORM` clause.

### Syntax

```sql
SELECT TRANSFORM ( expression [ , ... ] )
    [ ROW FORMAT row_format ]
    [ RECORDWRITER record_writer_class ]
    USING command_or_script [ AS ( [ col_name [ col_type ] ] [ , ... ] ) ]
    [ ROW FORMAT row_format ]
    [ RECORDREADER record_reader_class ]

row_format:    
    : SERDE serde_class [ WITH SERDEPROPERTIES (k1=v1, k2=v2, ... ) ]
    | DELIMITED [ FIELDS TERMINATED BY fields_terminated_char [ ESCAPED BY escaped_char ] ] 
        [ COLLECTION ITEMS TERMINATED BY collection_items_terminated_char ] 
        [ MAP KEYS TERMINATED BY map_key_terminated_char ]
        [ LINES TERMINATED BY row_terminated_char ]
        [ NULL DEFINED AS null_char ]
```

### Parameters

* **expression**
    
    Specifies a combination of one or more values, operators and SQL functions that results in a value.

* **row_format**    

    Spark uses the `SERDE` clause to specify a custom SerDe for one table. Otherwise, use the `DELIMITED` clause to use the native SerDe and specify the delimiter, escape character, null character and so on.

* **SERDE**

    Specifies a custom SerDe for one table.

* **serde_class**

    Specifies a fully-qualified class name of a custom SerDe.

* **DELIMITED**

    The `DELIMITED` clause can be used to specify the native SerDe and state the delimiter, escape character, null character and so on.

* **FIELDS TERMINATED BY**

    Used to define a column separator.
    
* **COLLECTION ITEMS TERMINATED BY**

    Used to define a collection item separator.
   
* **MAP KEYS TERMINATED BY**

    Used to define a map key separator.
    
* **LINES TERMINATED BY**

    Used to define a row separator.
    
* **NULL DEFINED AS**

    Used to define the specific value for NULL.
    
* **ESCAPED BY**

    Used for escape mechanism.

* **RECORDWRITER**

    Specifies a fully-qualified class name of a custom RecordWriter. The default value is `org.apache.hadoop.hive.ql.exec.TextRecordWriter`.

* **RECORDREADER**

    Specifies a fully-qualified class name of a custom RecordReader. The default value is `org.apache.hadoop.hive.ql.exec.TextRecordReader`.

* **command_or_script**

    Specifies a command or a path to script to process data.

### SerDe behavior

Spark uses Hive Serde `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe` by default, columns will be casted
to `STRING` and combined by tabs before feeding to the user script. All `NULL` values will be converted
to the literal string `"\N"` in order to differentiate `NULL` values from empty strings. The standard output of the
user script will be treated as TAB-separated STRING columns, any cell containing only `"\N"` will be re-interpreted
as a `NULL`, and then the resulting STRING column will be cast to the data type specified in the table declaration
in the usual way. If the actual number of output columns is less than the number of specified output columns,
insufficient output columns will be supplemented with `NULL`. If the actual number of output columns is more than the
number of specified output columns, the output columns will only select the corresponding columns and the remaining part
will be discarded. If there is no `AS` clause after `USING my_script`, Spark assumes that the output of the script contains 2 parts:

   1. key: which is before the first tab.
   2. value: which is the rest after the first tab.

Note that this is different from specifying an `AS key, value` because in that case, the value will only contain the portion
between the first tab and the second tab if there are multiple tabs. 
User scripts can output debug information to standard error which will be shown on the task detail
page on Spark. These defaults can be overridden with `ROW FORMAT SERDE` or `ROW FORMAT DELIMITED`. 

### Examples

```sql
CREATE TABLE person (zip_code INT, name STRING, age INT);
INSERT INTO person VALUES
    (94588, 'Zen Hui', 50),
    (94588, 'Dan Li', 18),
    (94588, 'Anil K', 27),
    (94588, 'John V', NULL),
    (94511, 'David K', 42),
    (94511, 'Aryan B.', 18),
    (94511, 'Lalit B.', NULL);

-- With specified output without data type
SELECT TRANSFORM(zip_code, name, age)
   USING 'cat' AS (a, b, c)
FROM person
WHERE zip_code > 94511;
+-------+---------+-----+
|    a  |        b|    c|
+-------+---------+-----+
|  94588|   Anil K|   27|
|  94588|   John V| NULL|
|  94588|  Zen Hui|   50|
|  94588|   Dan Li|   18|
+-------+---------+-----+

-- With specified output with data type
SELECT TRANSFORM(zip_code, name, age)
   USING 'cat' AS (a STRING, b STRING, c STRING)
FROM person
WHERE zip_code > 94511;
+-------+---------+-----+
|    a  |        b|    c|
+-------+---------+-----+
|  94588|   Anil K|   27|
|  94588|   John V| NULL|
|  94588|  Zen Hui|   50|
|  94588|   Dan Li|   18|
+-------+---------+-----+

-- Using ROW FORMAT DELIMITED
SELECT TRANSFORM(name, age)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    NULL DEFINED AS 'NULL'
    USING 'cat' AS (name_age string)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '@'
    LINES TERMINATED BY '\n'
    NULL DEFINED AS 'NULL'
FROM person;
+---------------+
|       name_age|
+---------------+
|      Anil K,27|
|    John V,null|
|     ryan B.,18|
|     David K,42|
|     Zen Hui,50|
|      Dan Li,18|
|  Lalit B.,null|
+---------------+

-- Using Hive Serde
SELECT TRANSFORM(zip_code, name, age)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    WITH SERDEPROPERTIES (
      'field.delim' = '\t'
    )
    USING 'cat' AS (a STRING, b STRING, c STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    WITH SERDEPROPERTIES (
      'field.delim' = '\t'
    )
FROM person
WHERE zip_code > 94511;
+-------+---------+-----+
|    a  |        b|    c|
+-------+---------+-----+
|  94588|   Anil K|   27|
|  94588|   John V| NULL|
|  94588|  Zen Hui|   50|
|  94588|   Dan Li|   18|
+-------+---------+-----+

-- Schema-less mode
SELECT TRANSFORM(zip_code, name, age)
    USING 'cat'
FROM person
WHERE zip_code > 94500;
+-------+---------------------+
|    key|                value|
+-------+---------------------+
|  94588|	  Anil K    27|
|  94588|	  John V    \N|
|  94511|	Aryan B.    18|
|  94511|	 David K    42|
|  94588|	 Zen Hui    50|
|  94588|	  Dan Li    18|
|  94511|	Lalit B.    \N|
+-------+---------------------+
```

### Related Statements

* [SELECT Main](sql-ref-syntax-qry-select.html)
* [WHERE Clause](sql-ref-syntax-qry-select-where.html)
* [GROUP BY Clause](sql-ref-syntax-qry-select-groupby.html)
* [HAVING Clause](sql-ref-syntax-qry-select-having.html)
* [ORDER BY Clause](sql-ref-syntax-qry-select-orderby.html)
* [SORT BY Clause](sql-ref-syntax-qry-select-sortby.html)
* [DISTRIBUTE BY Clause](sql-ref-syntax-qry-select-distribute-by.html)
* [LIMIT Clause](sql-ref-syntax-qry-select-limit.html)
* [CASE Clause](sql-ref-syntax-qry-select-case.html)
* [PIVOT Clause](sql-ref-syntax-qry-select-pivot.html)
* [LATERAL VIEW Clause](sql-ref-syntax-qry-select-lateral-view.html)
