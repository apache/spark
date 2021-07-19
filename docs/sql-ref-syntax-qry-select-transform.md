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
to transform the inputs by running a user-specified command or script.

Spark's script transform supports two modes:

    1. Without Hive: Spark script transform can run without `-Phive` or `SparkSession.builder.enableHiveSupport()`.
                     In this case, now spark only use script transform with `ROW FORMAT DELIMITED` and treat all values passed
                     to the script as strings. 
    2. With Hive: When built Spark with `-Phive` and started Spark SQL with `enableHiveSupport()`, spark can use script 
                  transform with Hive SerDe and both `ROW FORMAT DELIMITED`.

### Syntax

```sql
SELECT TRANSFORM ( expression [ , ... ] )
    [ ROW FORMAT row_format ]
    [ RECORDWRITER record_writer_class ]
    USING command_or_script [ AS ( [ col_name [ col_type ] ] [ , ... ] ) ]
    [ ROW FORMAT row_format ]
    [ RECORDREADER record_reader_class ]
```

### Parameters

* **expression**
    
    Specifies a combination of one or more values, operators and SQL functions that results in a value.

* **row_format** 

    Specifies the row format for input and output. See [HIVE FORMAT](sql-ref-syntax-hive-format.html) for more syntax details.

* **RECORDWRITER**

    Specifies a fully-qualified class name of a custom RecordWriter. The default value is `org.apache.hadoop.hive.ql.exec.TextRecordWriter`.

* **RECORDREADER**

    Specifies a fully-qualified class name of a custom RecordReader. The default value is `org.apache.hadoop.hive.ql.exec.TextRecordReader`.

* **command_or_script**

    Specifies a command or a path to script to process data.

### ROW FORMAT DELIMITED BEHAVIOR

When spark use `ROW FORMAT DELIMITED` format, Spark will use `\u0001` as default filed delimit,
use `\n` as default line delimit and use `"\N"` as `NULL` value in order to differentiate `NULL` values 
from empty strings. These delimit can be overridden by `FIELDS TERMINATED BY`, `LINES TERMINATED BY` and
`NULL TERMINATED AS`. Since we use `to_json` and `from_json` to handle complex data type, so 
`COLLECTION ITEMS TERMINATED BY` and `MAP KEYS TERMINATED BY` won't work in current code. 
Spark will cast all columns to `STRING` and combined by tabs before feeding to the user script.
For complex type such as `ARRAY\MAP\STRUCT`, spark use `to_json` cast it to input json string
and use `from_json` to convert result output to `ARRAY/MAP/STRUCT` data. The standard output of
the user script will be treated as tab-separated `STRING` columns, any cell containing only `"\N"` 
will be re-interpreted as a `NULL` value, and then the resulting STRING column will be cast to the 
data type specified in `col_type`. If the actual number of output columns is less than the number 
of specified output columns, insufficient output columns will be supplemented with `NULL`. 
If the actual number of output columns is more than the number of specified output columns,
the output columns will only select the corresponding columns and the remaining part will be discarded.

If there is no `AS` clause after `USING my_script`, an output schema will be `key: STRING, value: STRING`.
The `key` column contains all the characters before the first tab and the `value` column contains the remaining characters after the first tab.
If there is no enough tab, Spark will return `NULL` value. These defaults can be overridden with `ROW FORMAT SERDE` or `ROW FORMAT DELIMITED`. 

### Hive SerDe behavior

When enable Hive and use Hive Serde mode, spark uses the Hive SerDe `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe` by default, so columns will be casted
to `STRING` and combined by tabs before feeding to the user script. All `NULL` values will be converted
to the literal string `"\N"` in order to differentiate `NULL` values from empty strings. The standard output of the
user script will be treated as tab-separated `STRING` columns, any cell containing only `"\N"` will be re-interpreted
as a `NULL` value, and then the resulting STRING column will be cast to the data type specified in `col_type`. If the actual
number of output columns is less than the number of specified output columns, insufficient output columns will be
supplemented with `NULL`. If the actual number of output columns is more than the number of specified output columns,
the output columns will only select the corresponding columns and the remaining part will be discarded.

If there is no `AS` clause after `USING my_script`, an output schema will be `key: STRING, value: STRING`.
The `key` column contains all the characters before the first tab and the `value` column contains the remaining characters after the first tab.
If there is no enough tab, Spark will return `NULL` value. These defaults can be overridden with `ROW FORMAT SERDE` or `ROW FORMAT DELIMITED`. 

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
