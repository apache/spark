---
layout: global
title: Hive Row Format
displayTitle: Hive Row Format
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

Spark supports a Hive row format in `CREATE TABLE` and `TRANSFORM` clause to specify serde or text delimiter.
There are two ways to define a row format in `row_format` of `CREATE TABLE` and `TRANSFORM` clauses.
  1. `SERDE` clause to specify a custom SerDe class.
  2. `DELIMITED` clause to specify a delimiter, an escape character, a null character, and so on for the native SerDe.

### Syntax

```sql
row_format:    
    SERDE serde_class [ WITH SERDEPROPERTIES (k1=v1, k2=v2, ... ) ]
    | DELIMITED [ FIELDS TERMINATED BY fields_terminated_char [ ESCAPED BY escaped_char ] ] 
        [ COLLECTION ITEMS TERMINATED BY collection_items_terminated_char ] 
        [ MAP KEYS TERMINATED BY map_key_terminated_char ]
        [ LINES TERMINATED BY row_terminated_char ]
        [ NULL DEFINED AS null_char ]
```

### Parameters
   
* **SERDE serde_class**

    Specifies a fully-qualified class name of custom SerDe.

* **SERDEPROPERTIES**

    A list of key-value pairs that is used to tag the SerDe definition.

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
