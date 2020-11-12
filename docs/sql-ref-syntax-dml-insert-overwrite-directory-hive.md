---
layout: global
title: INSERT OVERWRITE DIRECTORY with Hive format
displayTitle: INSERT OVERWRITE DIRECTORY with Hive format
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

The `INSERT OVERWRITE DIRECTORY` with Hive format overwrites the existing data in the directory with the new values using Hive `SerDe`.
Hive support must be enabled to use this command. The inserted rows can be specified by value expressions or result from a query.

### Syntax

```sql
INSERT OVERWRITE [ LOCAL ] DIRECTORY directory_path
    [ ROW FORMAT row_format ] [ STORED AS file_format ]
    { VALUES ( { value | NULL } [ , ... ] ) [ , ( ... ) ] | query }
```

### Parameters

* **directory_path**

    Specifies the destination directory. The `LOCAL` keyword is used to specify that the directory is on the local file system.

* **row_format**

    Specifies the row format for this insert. Valid options are `SERDE` clause and `DELIMITED` clause. `SERDE` clause can be used to specify a custom `SerDe` for this insert. Alternatively, `DELIMITED` clause can be used to specify the native `SerDe` and state the delimiter, escape character, null character, and so on.

* **file_format**

    Specifies the file format for this insert. Valid options are `TEXTFILE`, `SEQUENCEFILE`, `RCFILE`, `ORC`, `PARQUET`, and `AVRO`. You can also specify your own input and output format using `INPUTFORMAT` and `OUTPUTFORMAT`. `ROW FORMAT SERDE` can only be used with `TEXTFILE`, `SEQUENCEFILE`, or `RCFILE`, while `ROW FORMAT DELIMITED` can only be used with `TEXTFILE`.

* **VALUES ( { value `|` NULL } [ , ... ] ) [ , ( ... ) ]**

    Specifies the values to be inserted. Either an explicitly specified value or a NULL can be inserted.
    A comma must be used to separate each value in the clause. More than one set of values can be specified to insert multiple rows.

* **query**

    A query that produces the rows to be inserted. It can be in one of following formats:
    * a `SELECT` statement
    * a `TABLE` statement
    * a `FROM` statement

### Examples

```sql
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination'
    STORED AS orc
    SELECT * FROM test_table;

INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    SELECT * FROM test_table;
```

### Related Statements

* [INSERT INTO statement](sql-ref-syntax-dml-insert-into.html)
* [INSERT OVERWRITE statement](sql-ref-syntax-dml-insert-overwrite-table.html)
* [INSERT OVERWRITE DIRECTORY statement](sql-ref-syntax-dml-insert-overwrite-directory.html)
