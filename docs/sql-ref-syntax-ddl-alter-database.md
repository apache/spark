---
layout: global
title: ALTER DATABASE
displayTitle: ALTER DATABASE
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

`ALTER DATABASE` statement changes the properties or location of a database. Please note that the usage of
`DATABASE`, `SCHEMA` and `NAMESPACE` are interchangeable and one can be used in place of the others. An error message
is issued if the database is not found in the system.

### SET PROPERTIES
`ALTER DATABASE SET DBPROPERTIES` statement changes the properties associated with a database.
The specified property values override any existing value with the same property name. 
This command is mostly used to record the metadata for a database and may be used for auditing purposes.

#### Syntax

```sql
ALTER { DATABASE | SCHEMA | NAMESPACE } database_name
    SET { DBPROPERTIES | PROPERTIES } ( property_name = property_value [ , ... ] )
```

#### Parameters

* **database_name**

    Specifies the name of the database to be altered.

### UNSET PROPERTIES
`ALTER DATABASE UNSET DBPROPERTIES` statement unsets the properties associated with a database.
If the specified property key does not exist, the command will ignore it and finally succeed.
(available since Spark 4.0.0).

#### Syntax

```sql
ALTER { DATABASE | SCHEMA | NAMESPACE } database_name
    UNSET { DBPROPERTIES | PROPERTIES } ( property_name [ , ... ] )
```

#### Parameters

* **database_name**

  Specifies the name of the database to be altered.

### SET LOCATION
`ALTER DATABASE SET LOCATION` statement changes the default parent-directory where new tables will be added 
for a database. Please note that it does not move the contents of the database's current directory to the newly 
specified location or change the locations associated with any tables/partitions under the specified database 
(available since Spark 3.0.0 with the Hive metastore version 3.0.0 and later).

#### Syntax

```sql
ALTER { DATABASE | SCHEMA | NAMESPACE } database_name
    SET LOCATION 'new_location'
```

#### Parameters

* **database_name**

  Specifies the name of the database to be altered.

### Examples

```sql
-- Creates a database named `inventory`.
CREATE DATABASE inventory;

-- Alters the database to set properties `Edited-by` and `Edit-date`.
ALTER DATABASE inventory SET DBPROPERTIES ('Edited-by' = 'John', 'Edit-date' = '01/01/2001');

-- Verify that properties are set.
DESCRIBE DATABASE EXTENDED inventory;
+-------------------------+------------------------------------------+
|database_description_item|                database_description_value|
+-------------------------+------------------------------------------+
|            Database Name|                                 inventory|
|              Description|                                          |
|                 Location|   file:/temp/spark-warehouse/inventory.db|
|               Properties|((Edit-date,01/01/2001), (Edited-by,John))|
+-------------------------+------------------------------------------+

-- Alters the database to set a new location.
ALTER DATABASE inventory SET LOCATION 'file:/temp/spark-warehouse/new_inventory.db';

-- Verify that a new location is set.
DESCRIBE DATABASE EXTENDED inventory;
+-------------------------+-------------------------------------------+
|database_description_item|                 database_description_value|
+-------------------------+-------------------------------------------+
|            Database Name|                                  inventory|
|              Description|                                           |
|                 Location|file:/temp/spark-warehouse/new_inventory.db|
|               Properties| ((Edit-date,01/01/2001), (Edited-by,John))|
+-------------------------+-------------------------------------------+

-- Alters the database to unset the property `Edited-by`
ALTER DATABASE inventory UNSET DBPROPERTIES ('Edited-by');

-- Verify that the property `Edited-by` has been unset.
DESCRIBE DATABASE EXTENDED inventory;
+-------------------------+-------------------------------------------+
|database_description_item|                 database_description_value|
+-------------------------+-------------------------------------------+
|            Database Name|                                  inventory|
|              Description|                                           |
|                 Location|file:/temp/spark-warehouse/new_inventory.db|
|               Properties| ((Edit-date,01/01/2001))                  |
+-------------------------+-------------------------------------------+

-- Alters the database to unset a non-existent property `non-existent`
-- Note: The command will ignore 'non-existent' and finally succeed
ALTER DATABASE inventory UNSET DBPROPERTIES ('non-existent');
```

### Related Statements

* [DESCRIBE DATABASE](sql-ref-syntax-aux-describe-database.html)
