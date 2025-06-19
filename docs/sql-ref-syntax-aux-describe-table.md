---
layout: global
title: DESCRIBE TABLE
displayTitle: DESCRIBE TABLE
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

`DESCRIBE TABLE` statement returns the basic metadata information of a
table. The metadata information includes column name, column type
and column comment. Optionally a partition spec or column name may be specified
to return the metadata pertaining to a partition or column respectively.

### Syntax

```sql
{ DESC | DESCRIBE } [ TABLE ] [ format ] table_identifier [ partition_spec ] [ col_name ] [ AS JSON ]
```

### Parameters

* **format**

    Specifies the optional format of describe output. If `EXTENDED` or `FORMATTED` is specified
    then additional metadata information (such as parent database, owner, and access time)
    is returned. Also if `EXTENDED` or `FORMATTED` is specified, then the metadata can be returned 
    in JSON format by specifying `AS JSON` at the end of the statement.

* **table_identifier**

    Specifies a table name, which may be optionally qualified with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **partition_spec**

    An optional parameter that specifies a comma separated list of key and value pairs
    for partitions. When specified, additional partition metadata is returned.

    **Syntax:** `PARTITION ( partition_col_name  = partition_col_val [ , ... ] )`

* **col_name**

    An optional parameter that specifies the column name that needs to be described.
    The supplied column name may be optionally qualified. Parameters `partition_spec`
    and `col_name` are  mutually exclusive and can not be specified together. Currently
    nested columns are not allowed to be specified.

    JSON format is not currently supported for individual columns.

    **Syntax:** `[ database_name. ] [ table_name. ] column_name`

* **AS JSON**

  An optional parameter to return the table metadata in JSON format. Only supported when `EXTENDED`
  or `FORMATTED` format is specified (both produce equivalent JSON).

  **Syntax:** `[ AS JSON ]`

  **Schema:**

  Below is the full JSON schema.
  In actual output, null fields are omitted and the JSON is not pretty-printed (see Examples).

  ```sql
    {
      "table_name": "<table_name>",
      "catalog_name": "<catalog_name>",
      "schema_name": "<innermost_namespace_name>",
      "namespace": ["<namespace_names>"],
      "type": "<table_type>",
      "provider": "<provider>",
      "columns": [
        {
          "name": "<name>",
          "type": <type_json>,
          "comment": "<comment>",
          "nullable": <boolean>,
          "default": "<default_val>"
        }
      ],
      "partition_values": {
        "<col_name>": "<val>"
      },
      "partition_columns": ["col1", "col2"],
      "clustering_columns": ["col1", "col2"],
      "location": "<path>",
      "view_text": "<view_text>",
      "view_original_text": "<view_original_text>",
      "view_schema_mode": "<view_schema_mode>",
      "view_catalog_and_namespace": "<view_catalog_and_namespace>",
      "view_query_output_columns": ["col1", "col2"],
      // Spark SQL configurations captured at the time of permanent view creation
      "view_creation_spark_configuration": {
        "conf1": "<value1>",
        "conf2": "<value2>"
      },
      "comment": "<comment>",
      "table_properties": {
        "property1": "<property1>",
        "property2": "<property2>"
      },
      "storage_properties": {
        "property1": "<property1>",
        "property2": "<property2>"
      },
      "serde_library": "<serde_library>",
      "input_format": "<input_format>",
      "output_format": "<output_format>",
      "num_buckets": <num_buckets>,
      "bucket_columns": ["<col_name>"],
      "sort_columns": ["<col_name>"],
      "created_time": "<yyyy-MM-dd'T'HH:mm:ss'Z'>",
      "created_by": "<created_by>",
      "last_access": "<yyyy-MM-dd'T'HH:mm:ss'Z'>",
      "partition_provider": "<partition_provider>",
      "collation": "<default_collation>"
    }
  ```
  
  Below are the schema definitions for `<type_json>`:

| Spark SQL Data Types  | JSON Representation                                                                                                                                              |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ByteType              | `{ "name" : "tinyint" }`                                                                                                                                         |
| ShortType             | `{ "name" : "smallint" }`                                                                                                                                        |
| IntegerType           | `{ "name" : "int" }`                                                                                                                                             |
| LongType              | `{ "name" : "bigint" }`                                                                                                                                          |
| FloatType             | `{ "name" : "float" }`                                                                                                                                           |
| DoubleType            | `{ "name" : "double" }`                                                                                                                                          |
| DecimalType           | `{ "name" : "decimal", "precision": p, "scale": s }`                                                                                                             |
| StringType            | `{ "name" : "string", "collation": "<collation>" }`                                                                                                                                          |
| VarCharType           | `{ "name" : "varchar", "length": n }`                                                                                                                            |
| CharType              | `{ "name" : "char", "length": n }`                                                                                                                               |
| BinaryType            | `{ "name" : "binary" }`                                                                                                                                          |
| BooleanType           | `{ "name" : "boolean" }`                                                                                                                                         |
| DateType              | `{ "name" : "date" }`                                                                                                                                            |
| VariantType           | `{ "name" : "variant" }`                                                                                                                                         |
| TimestampType         | `{ "name" : "timestamp_ltz" }`                                                                                                                                   |
| TimestampNTZType      | `{ "name" : "timestamp_ntz" }`                                                                                                                                   |
| YearMonthIntervalType | `{ "name" : "interval", "start_unit": "<start_unit>", "end_unit": "<end_unit>" }`                                                                                |
| DayTimeIntervalType   | `{ "name" : "interval", "start_unit": "<start_unit>", "end_unit": "<end_unit>" }`                                                                                |
| ArrayType             | `{ "name" : "array", "element_type": <type_json>, "element_nullable": <boolean> }`                                                                               |
| MapType               | `{ "name" : "map", "key_type": <type_json>, "value_type": <type_json>, "value_nullable": <boolean> }`                                                            |
| StructType            | `{ "name" : "struct", "fields": [ {"name" : "field1", "type" : <type_json>, “nullable”: <boolean>, "comment": “<comment>”, "default": “<default_val>”}, ... ] }` |

### Examples

```sql
-- Creates a table `customer`. Assumes current database is `salesdb`.
CREATE TABLE customer(
        cust_id INT,
        state VARCHAR(20),
        name STRING COMMENT 'Short name'
    )
    USING parquet
    PARTITIONED BY (state);
    
INSERT INTO customer PARTITION (state = 'AR') VALUES (100, 'Mike');
    
-- Returns basic metadata information for unqualified table `customer`
DESCRIBE TABLE customer;
+-----------------------+---------+----------+
|               col_name|data_type|   comment|
+-----------------------+---------+----------+
|                cust_id|      int|      null|
|                   name|   string|Short name|
|                  state|   string|      null|
|# Partition Information|         |          |
|             # col_name|data_type|   comment|
|                  state|   string|      null|
+-----------------------+---------+----------+

-- Returns basic metadata information for qualified table `customer`
DESCRIBE TABLE salesdb.customer;
+-----------------------+---------+----------+
|               col_name|data_type|   comment|
+-----------------------+---------+----------+
|                cust_id|      int|      null|
|                   name|   string|Short name|
|                  state|   string|      null|
|# Partition Information|         |          |
|             # col_name|data_type|   comment|
|                  state|   string|      null|
+-----------------------+---------+----------+

-- Returns additional metadata such as parent database, owner, access time etc.
DESCRIBE TABLE EXTENDED customer;
+----------------------------+------------------------------+----------+
|                    col_name|                     data_type|   comment|
+----------------------------+------------------------------+----------+
|                     cust_id|                           int|      null|
|                        name|                        string|Short name|
|                       state|                        string|      null|
|     # Partition Information|                              |          |
|                  # col_name|                     data_type|   comment|
|                       state|                        string|      null|
|                            |                              |          |
|# Detailed Table Information|                              |          |
|                    Database|                       default|          |
|                       Table|                      customer|          |
|                       Owner|                 <TABLE OWNER>|          |
|                Created Time|  Tue Apr 07 22:56:34 JST 2020|          |
|                 Last Access|                       UNKNOWN|          |
|                  Created By|               <SPARK VERSION>|          |
|                        Type|                       MANAGED|          |
|                    Provider|                       parquet|          |
|                    Location|file:/tmp/salesdb.db/custom...|          |
|               Serde Library|org.apache.hadoop.hive.ql.i...|          |
|                 InputFormat|org.apache.hadoop.hive.ql.i...|          |
|                OutputFormat|org.apache.hadoop.hive.ql.i...|          |
|          Partition Provider|                       Catalog|          |
+----------------------------+------------------------------+----------+

-- Returns partition metadata such as partitioning column name, column type and comment.
DESCRIBE TABLE EXTENDED customer PARTITION (state = 'AR');
+------------------------------+------------------------------+----------+
|                      col_name|                     data_type|   comment|
+------------------------------+------------------------------+----------+
|                       cust_id|                           int|      null|
|                          name|                        string|Short name|
|                         state|                        string|      null|
|       # Partition Information|                              |          |
|                    # col_name|                     data_type|   comment|
|                         state|                        string|      null|
|                              |                              |          |
|# Detailed Partition Inform...|                              |          |
|                      Database|                       default|          |
|                         Table|                      customer|          |
|              Partition Values|                    [state=AR]|          |
|                      Location|file:/tmp/salesdb.db/custom...|          |
|                 Serde Library|org.apache.hadoop.hive.ql.i...|          |
|                   InputFormat|org.apache.hadoop.hive.ql.i...|          |
|                  OutputFormat|org.apache.hadoop.hive.ql.i...|          |
|            Storage Properties|[serialization.format=1, pa...|          |
|          Partition Parameters|{transient_lastDdlTime=1586...|          |
|                  Created Time|  Tue Apr 07 23:05:43 JST 2020|          |
|                   Last Access|                       UNKNOWN|          |
|          Partition Statistics|                     659 bytes|          |
|                              |                              |          |
|         # Storage Information|                              |          |
|                      Location|file:/tmp/salesdb.db/custom...|          |
|                 Serde Library|org.apache.hadoop.hive.ql.i...|          |
|                   InputFormat|org.apache.hadoop.hive.ql.i...|          |
|                  OutputFormat|org.apache.hadoop.hive.ql.i...|          |
+------------------------------+------------------------------+----------+

-- Returns the metadata for `name` column.
-- Optional `TABLE` clause is omitted and column is fully qualified.
DESCRIBE customer salesdb.customer.name;
+---------+----------+
|info_name|info_value|
+---------+----------+
| col_name|      name|
|data_type|    string|
|  comment|Short name|
+---------+----------+

-- Returns the table metadata in JSON format.
DESC FORMATTED customer AS JSON;
{"table_name":"customer","catalog_name":"spark_catalog","schema_name":"default","namespace":["default"],"columns":[{"name":"cust_id","type":{"name":"integer"},"nullable":true},{"name":"name","type":{"name":"string"},"comment":"Short name","nullable":true},{"name":"state","type":{"name":"varchar","length":20},"nullable":true}],"location": "file:/tmp/salesdb.db/custom...","created_time":"2020-04-07T14:05:43Z","last_access":"UNKNOWN","created_by":"None","type":"MANAGED","provider":"parquet","partition_provider":"Catalog","partition_columns":["state"]}
```

### Related Statements

* [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)
* [DESCRIBE QUERY](sql-ref-syntax-aux-describe-query.html)
* [DESCRIBE FUNCTION](sql-ref-syntax-aux-describe-function.html)
