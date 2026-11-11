---
layout: global
title: SHOW TBLPROPERTIES
displayTitle: SHOW TBLPROPERTIES
---

### Description

This statement returns the value of a table property given an optional value for
a property key. If no key is specified then all the properties are returned. 

### Syntax

```sql
SHOW TBLPROPERTIES table_identifier 
   [ ( unquoted_property_key | property_key_as_string_literal ) ]
   [ AS JSON ]
```

### Parameters

* **table_identifier**

    Specifies the table name of an existing table. The table may be optionally qualified
    with a database name.

    **Syntax:** `[ database_name. ] table_name`

* **unquoted_property_key**

    Specifies the property key in unquoted form. The key may consists of multiple
    parts separated by dot.

    **Syntax:** `[ key_part1 ] [ .key_part2 ] [ ... ]`

* **property_key_as_string_literal**

    Specifies a property key value as a string literal.

* **AS JSON**

    An optional parameter to return the table properties as a single-row JSON document
    instead of the default tabular format. Supported for both V1 (session catalog / Hive
    metastore) and V2 tables and views.

    **Syntax:** `[ AS JSON ]`

    **Output schema:** A single column named `json_metadata` of type `STRING NOT NULL`.

    **Schema:**

    Below is the full JSON schema.
    ```json
    {
      "properties": [{"key": "k", "value": "v"}, ...]
    }
    ```

    | Field | Type | Description |
    |---|---|---|
    | `properties` | array of objects | Each element maps a property `key` to its string `value` and If specific property key is requested and it is not set on the table, the array is empty. Sorted by key when all properties are listed.  |


### Examples

```sql
-- create a table `customer` in database `salesdb`
USE salesdb;
CREATE TABLE customer(cust_code INT, name VARCHAR(100), cust_addr STRING)
    TBLPROPERTIES ('created.by.user' = 'John', 'created.date' = '01-01-2001');

-- show all the user specified properties for table `customer`
SHOW TBLPROPERTIES customer;
+---------------------+----------+
|                  key|     value|
+---------------------+----------+
|      created.by.user|      John|
|         created.date|01-01-2001|
|transient_lastDdlTime|1567554931|
+---------------------+----------+

-- show all the user specified properties for a qualified table `customer`
-- in database `salesdb`
SHOW TBLPROPERTIES salesdb.customer;
+---------------------+----------+
|                  key|     value|
+---------------------+----------+
|      created.by.user|      John|
|         created.date|01-01-2001|
|transient_lastDdlTime|1567554931|
+---------------------+----------+

-- show value for unquoted property key `created.by.user`
SHOW TBLPROPERTIES customer (created.by.user);
+-----+
|value|
+-----+
| John|
+-----+

-- show value for property `created.date`` specified as string literal
SHOW TBLPROPERTIES customer ('created.date');
+----------+
|     value|
+----------+
|01-01-2001|
+----------+

-- show all the user specified properties for table `customer` as JSON
SHOW TBLPROPERTIES customer AS JSON;
+-----------------------------------------------------------------------------------------------------+
|json_metadata                                                                                         |
+-----------------------------------------------------------------------------------------------------+
|{"properties":[{"key":"created.by.user","value":"John"},{"key":"created.date","value":"01-01-2001"}]}|
+-----------------------------------------------------------------------------------------------------+

-- show value for a specific property key as JSON
SHOW TBLPROPERTIES customer ('created.date') AS JSON;
+------------------------------------------------------------+
|json_metadata                                                |
+------------------------------------------------------------+
|{"properties":[{"key":"created.date","value":"01-01-2001"}]}|
+------------------------------------------------------------+

-- when the requested key is not set, AS JSON returns an empty array
SHOW TBLPROPERTIES customer ('does.not.exist') AS JSON;
+-----------------+
|json_metadata    |
+-----------------+
|{"properties":[]}|
+-----------------+
```

### Related Statements

* [SHOW CREATE TABLE](sql-ref-syntax-aux-show-create-table.md)
