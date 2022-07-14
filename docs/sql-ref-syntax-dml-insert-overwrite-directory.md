---
layout: global
title: INSERT OVERWRITE DIRECTORY
displayTitle: INSERT OVERWRITE DIRECTORY
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

The `INSERT OVERWRITE DIRECTORY` statement overwrites the existing data in the directory with the new values using either spark file format or Hive Serde. 
Hive support must be enabled to use Hive Serde. The inserted rows can be specified by value expressions or result from a query.

### Syntax

```sql
INSERT OVERWRITE [ LOCAL ] DIRECTORY [ directory_path ]
    { spark_format | hive_format }
    { VALUES ( { value | NULL } [ , ... ] ) [ , ( ... ) ] | query }
```

While `spark_format` is defined as
```sql
USING file_format [ OPTIONS ( key = val [ , ... ] ) ]
```

`hive_format` is defined as
```sql
[ ROW FORMAT row_format ] [ STORED AS hive_serde ]
```

### Parameters

* **directory_path**

    Specifies the destination directory. The `LOCAL` keyword is used to specify that the directory is on the local file system.
    In spark file format, it can also be specified in `OPTIONS` using `path`, but `directory_path` and `path` option can not be both specified.

* **file_format**

    Specifies the file format to use for the insert. Valid options are `TEXT`, `CSV`, `JSON`, `JDBC`, `PARQUET`, `ORC`, `HIVE`, `LIBSVM`, or a fully qualified class name of a custom implementation of `org.apache.spark.sql.execution.datasources.FileFormat`.

* **OPTIONS ( key = val [ , ... ] )**

    Specifies one or more options for the writing of the file format.

* **hive_format**

    Specifies the file format to use for the insert. Both `row_format` and `hive_serde` are optional. `ROW FORMAT SERDE` can only be used with `TEXTFILE`, `SEQUENCEFILE`, or `RCFILE`, while `ROW FORMAT DELIMITED` can only be used with `TEXTFILE`. If both are not defined, spark uses `TEXTFILE`.

* **row_format**

    Specifies the row format for this insert. Valid options are `SERDE` clause and `DELIMITED` clause. `SERDE` clause can be used to specify a custom `SerDe` for this insert. Alternatively, `DELIMITED` clause can be used to specify the native `SerDe` and state the delimiter, escape character, null character, and so on.

* **hive_serde**

    Specifies the file format for this insert. Valid options are `TEXTFILE`, `SEQUENCEFILE`, `RCFILE`, `ORC`, `PARQUET`, and `AVRO`. You can also specify your own input and output format using `INPUTFORMAT` and `OUTPUTFORMAT`.

* **VALUES ( { value `|` NULL } [ , ... ] ) [ , ( ... ) ]**

    Specifies the values to be inserted. Either an explicitly specified value or a NULL can be inserted.
    A comma must be used to separate each value in the clause. More than one set of values can be specified to insert multiple rows.

* **query**

    A query that produces the rows to be inserted. It can be in one of following formats:
    * a [SELECT](sql-ref-syntax-qry-select.html) statement
    * a [Inline Table](sql-ref-syntax-qry-select-inline-table.html) statement
    * a `FROM` statement

### Examples

#### Spark format
```sql
INSERT OVERWRITE DIRECTORY '/tmp/destination'
    USING parquet
    OPTIONS (col1 1, col2 2, col3 'test')
    SELECT * FROM test_table;

INSERT OVERWRITE DIRECTORY
    USING parquet
    OPTIONS ('path' '/tmp/destination', col1 1, col2 2, col3 'test')
    SELECT * FROM test_table;
```

#### Hive format

```sql
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination'
    STORED AS orc
    SELECT * FROM test_table;

INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    SELECT * FROM test_table;
```

### Related Statements

* [INSERT TABLE statement](sql-ref-syntax-dml-insert-table.html)
