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

The `TRANSFORM` clause is used to specifies a hive-style transform (SELECT TRANSFORM/MAP/REDUCE)
query specification to transform the input by forking and running the specified script. Users can
plug in their own custom mappers and reducers in the data stream by using features natively supported
in the Spark/Hive language. e.g. in order to run a custom mapper script - map_script - and a custom
reducer script - reduce_script - the user can issue the following command which uses the TRANSFORM
clause to embed the mapper and the reducer scripts.

Currently, Spark's script transform support two mode:

    1. Without Hive: It means we run Spark SQL without hive support, in this mode, we can use default format 
       by treating data as STRING and use Spark's own SerDe.
    2. WIth Hive: It means we run Spark SQL with Hive support, in this mode, when we use default format, 
       it will be treated as Hive default fomat. And we can use Hive supported SerDe to process data.

In both mode with default format, columns will be transformed to STRING and delimited by TAB before feeding
to the user script, Similarly, all NULL values will be converted to the literal string \N in order to
differentiate NULL values from empty strings. The standard output of the user script will be treated as
TAB-separated STRING columns, any cell containing only \N will be re-interpreted as a NULL, and then the
resulting STRING column will be cast to the data type specified in the table declaration in the usual way.
User scripts can output debug information to standard error which will be shown on the task detail page on hadoop.
These defaults can be overridden with `ROW FORMAT DELIMITED`.

### Syntax

```sql
rowFormat
    : ROW FORMAT SERDE serde_name [ WITH SERDEPROPERTIES serde_props ]
    | ROW FORMAT DELIMITED
       [ FIELDS TERMINATED BY fieldsTerminatedBy [ ESCAPED BY escapedBy ] ]
       [ COLLECTION ITEMS TERMINATED BY collectionItemsTerminatedBy ]
       [ MAP KEYS TERMINATED BY keysTerminatedBy ]
       [ LINES TERMINATED BY linesSeparatedBy ]
       [ NULL DEFINED AS nullDefinedAs ]  

inRowFormat=rowFormat
outRowFormat=rowFormat
namedExpressionSeq = named_expression [ , ... ]

transformClause:
  SELECT [ TRANSFORM ( namedExpressionSeq ) | MAP namedExpressionSeq | REDUCE namedExpressionSeq ]
    [ inRowFormat ]
    [ RECORDWRITER recordWriter ]
    USING script
    [ AS ( [ identifierSeq | colTypeList ] [ , ... ] ) ]
    [ outRowFormat ]
    [ RECORDREADER recordReader ]
  [ WHERE boolean_expression  ]
  [ GROUP BY expression [ , ... ] ]
  [ HAVING boolean_expression ]
```

### Parameters

* **named_expression**

    xxx.

* **serde_name**

    xxx.

* **serde_props**

    xxx.

* **fieldsTerminatedBy**

    xxx.

* **escapedBy**

    xxx.

* **collectionItemsTerminatedBy**

    xxx.

* **keysTerminatedBy**

    xxx.

* **linesSeparatedBy**

    xxx.

* **nullDefinedAs**

    xxx.

* **rowFormat**

    xxx.

* **recordWriter**

    xxx.

* **recordReader**

    xxx.
    
* **identifierSeq**
    
    xxx.
    
* **recordReader**
 
    xxx.

### Without Hive support Mode

### With Hive Support Mode

### Schema-less Script Transforms

If there is no AS clause after USING my_script, Spark assumes that the output of the script contains 2 parts:

   1. key: which is before the first tab, 
   2. value: which is the rest after the first tab.

Note that this is different from specifying AS key, value because in that case, value will only contain the portion
between the first tab and the second tab if there are multiple tabs. 

### Examples

```sql

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
