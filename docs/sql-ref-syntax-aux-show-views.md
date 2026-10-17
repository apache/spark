---
layout: global
title: SHOW VIEWS
displayTitle: SHOW VIEWS
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

The `SHOW VIEWS` statement returns all the views for an optionally specified database.
Additionally, the output of this statement may be filtered by an optional matching
pattern. If no database is specified then the views are returned from the 
current database. If the specified database is global temporary view database, we will
list global temporary views. Note that the command also lists local temporary views 
regardless of a given database.

### Syntax
```sql
SHOW VIEWS [ { FROM | IN } database_name ] [ LIKE regex_pattern ] [ AS JSON ]
```

### Parameters
* **{ FROM `|` IN } database_name**

     Specifies the database name from which views are listed.

* **regex_pattern**

     Specifies the regular expression pattern that is used to filter out unwanted views.

     * Except for `*` and `|` character, the pattern works like a regular expression.
     * `*` alone matches 0 or more characters and `|` is used to separate multiple different regular expressions,
       any of which can match.
     * The leading and trailing blanks are trimmed in the input pattern before processing. The pattern match is case-insensitive.

* **AS JSON**

     An optional parameter to return the view list as a single-row JSON document instead of the
     default tabular format.

     **Syntax:** `[ AS JSON ]`

     **Output schema:** A single column named `json_metadata` of type `STRING NOT NULL`.

     **Schema:**

     Below is the full JSON schema.
     In actual output, the JSON is not pretty-printed (see Examples).

     ```json
     {
       "views": [
         {
           "namespace": ["database_name", ...],
           "viewName": "view_name",
           "isTemporary": false
         }, ...
       ]
     }
     ```

     | Field | Type | Description |
     |---|---|---|
     | `views` | array of objects | One element per view. The array is empty when no view matches. Elements appear in the order returned by the underlying catalog. |
     | `namespace` | array of strings | The multi-part namespace holding the view. Empty for a local temporary view, which belongs to no database. |
     | `viewName` | string | The name of the view. |
     | `isTemporary` | boolean | Whether the view is a temporary view. |

### Examples
```sql
-- Create views in different databases, also create global/local temp views.
CREATE VIEW sam AS SELECT id, salary FROM employee WHERE name = 'sam';
CREATE VIEW sam1 AS SELECT id, salary FROM employee WHERE name = 'sam1';
CREATE VIEW suj AS SELECT id, salary FROM employee WHERE name = 'suj';
USE userdb;
CREATE VIEW user1 AS SELECT id, salary FROM default.employee WHERE name = 'user1';
CREATE VIEW user2 AS SELECT id, salary FROM default.employee WHERE name = 'user2';
USE default;
CREATE GLOBAL TEMP VIEW temp1 AS SELECT 1 AS col1;
CREATE TEMP VIEW temp2 AS SELECT 1 AS col1;

-- List all views in default database
SHOW VIEWS;
+-------------+------------+--------------+
| namespace   | viewName   | isTemporary  |
+-------------+------------+--------------+
| default     | sam        | false        |
| default     | sam1       | false        |
| default     | suj        | false        |
|             | temp2      | true         |
+-------------+------------+--------------+

-- List all views from userdb database 
SHOW VIEWS FROM userdb;
+-------------+------------+--------------+
| namespace   | viewName   | isTemporary  |
+-------------+------------+--------------+
| userdb      | user1      | false        |
| userdb      | user2      | false        |
|             | temp2      | true         |
+-------------+------------+--------------+
  
-- List all views in global temp view database 
SHOW VIEWS IN global_temp;
+-------------+------------+--------------+
| namespace   | viewName   | isTemporary  |
+-------------+------------+--------------+
| global_temp | temp1      | true         |
|             | temp2      | true         |
+-------------+------------+--------------+

-- List all views from default database matching the pattern `sam*`
SHOW VIEWS FROM default LIKE 'sam*';
+-----------+------------+--------------+
| namespace | viewName   | isTemporary  |
+-----------+------------+--------------+
| default   | sam        | false        |
| default   | sam1       | false        |
+-----------+------------+--------------+

-- List all views from the current database matching the pattern `sam|suj|temp*`
SHOW VIEWS LIKE 'sam|suj|temp*';
+-------------+------------+--------------+
| namespace   | viewName   | isTemporary  |
+-------------+------------+--------------+
| default     | sam        | false        |
| default     | suj        | false        |
|             | temp2      | true         |
+-------------+------------+--------------+

-- List all views from default database matching the pattern `sam*` in JSON format
SHOW VIEWS FROM default LIKE 'sam*' AS JSON;
+------------------------------------------------------------------------------------------------------------------------------------------+
|json_metadata                                                                                                                             |
+------------------------------------------------------------------------------------------------------------------------------------------+
|{"views":[{"namespace":["default"],"viewName":"sam","isTemporary":false},{"namespace":["default"],"viewName":"sam1","isTemporary":false}]}|
+------------------------------------------------------------------------------------------------------------------------------------------+

-- List all views in global temp view database in JSON format
SHOW VIEWS IN global_temp AS JSON;
+--------------------------------------------------------------------------------------------------------------------------------------+
|json_metadata                                                                                                                         |
+--------------------------------------------------------------------------------------------------------------------------------------+
|{"views":[{"namespace":["global_temp"],"viewName":"temp1","isTemporary":true},{"namespace":[],"viewName":"temp2","isTemporary":true}]}|
+--------------------------------------------------------------------------------------------------------------------------------------+

-- When no view matches, AS JSON returns an empty array
SHOW VIEWS FROM default LIKE 'no_such_view' AS JSON;
+-------------+
|json_metadata|
+-------------+
|{"views":[]} |
+-------------+
```

### Related statements
* [CREATE VIEW](sql-ref-syntax-ddl-create-view.html)
* [DROP VIEW](sql-ref-syntax-ddl-drop-view.html)
* [CREATE DATABASE](sql-ref-syntax-ddl-create-database.html)
* [DROP DATABASE](sql-ref-syntax-ddl-drop-database.html)
