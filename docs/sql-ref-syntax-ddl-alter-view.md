---
layout: global
title: ALTER VIEW
displayTitle: ALTER VIEW 
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

The `ALTER VIEW` statement can alter metadata associated with the view. It can change the definition of the view, change
the name of a view to a different name, set and unset the metadata of the view by setting `TBLPROPERTIES`.

#### RENAME View
Renames the existing view. If the new view name already exists in the source database, a `TableAlreadyExistsException` is thrown. This operation
does not support moving the views across databases.

#### Syntax
{% highlight sql %}
ALTER VIEW view_identifier RENAME TO view_identifier
{% endhighlight %}

#### Parameters
<dl>
  <dt><code><em>view_identifier</em></code></dt>
  <dd>
    Specifies a view name, which may be optionally qualified with a database name.<br><br>
    <b> Syntax:</b>
      <code>
        [ database_name. ] view_name
      </code>
  </dd>
</dl>

#### SET View Properties
Set one or more properties of an existing view. The properties are the key value pairs. If the properties' keys exist, 
the values are replaced with the new values. If the properties' keys do not exist, the key value pairs are added into
the properties.

#### Syntax
{% highlight sql %}
ALTER VIEW view_identifier SET TBLPROPERTIES ( property_key = property_val [ , ... ] )
{% endhighlight %}

#### Parameters
<dl>
  <dt><code><em>view_identifier</em></code></dt>
  <dd>
    Specifies a view name, which may be optionally qualified with a database name.<br><br>
    <b> Syntax:</b>
      <code>
        [ database_name. ] view_name
      </code>
  </dd>
  <dt><code><em>property_key</em></code></dt>
  <dd>
    Specifies the property key. The key may consists of multiple parts separated by dot.<br><br>
    <b>Syntax:</b>
      <code>
        [ key_part1 ] [ .key_part2 ] [ ... ]
      </code>
  </dd>
</dl>

#### UNSET View Properties
Drop one or more properties of an existing view. If the specified keys do not exist, an exception is thrown. Use 
`IF EXISTS` to avoid the exception. 

#### Syntax
{% highlight sql %}
ALTER VIEW view_identifier UNSET TBLPROPERTIES [ IF EXISTS ]  ( property_key [ , ... ] )
{% endhighlight %}

#### Parameters
<dl>
  <dt><code><em>view_identifier</em></code></dt>
  <dd>
    Specifies a view name, which may be optionally qualified with a database name.<br><br>
    <b> Syntax:</b>
      <code>
        [ database_name. ] view_name
      </code>
  </dd>
  <dt><code><em>property_key</em></code></dt>
  <dd>
    Specifies the property key. The key may consists of multiple parts separated by dot.<br><br>
    <b>Syntax:</b>
      <code>
        [ key_part1 ] [ .key_part2 ] [ ... ]
      </code>
  </dd>
</dl>

#### ALTER View AS SELECT
`ALTER VIEW view_identifier AS SELECT` statement changes the definition of a view, the `SELECT` statement must be valid,
and the `view_identifier` must exist.

#### Syntax
{% highlight sql %}
ALTER VIEW view_identifier AS select_statement
{% endhighlight %}

#### Parameters
<dl>
  <dt><code><em>view_identifier</em></code></dt>
  <dd>
    Specifies a view name, which may be optionally qualified with a database name.<br><br>
    <b> Syntax:</b>
      <code>
        [ database_name. ] view_name
      </code>
  </dd>
  <dt><code><em>select_statement</em></code></dt>
  <dd>
    Specifies the definition of the view, detail check <a href="sql-ref-syntax-qry-select.html">select_statement</a>
  </dd>
</dl>

### Examples

{% highlight sql %}
-- Rename only changes the view name.
-- The source and target databases of the view have to be the same.
-- Use qualified or unqualified name for the source and target view  
ALTER VIEW tempdb1.v1 RENAME TO tempdb1.v2;

-- Verify that the new view is created.
DESCRIBE TABLE EXTENDED tempdb1.v2;

+----------------------------+----------+-------+
|col_name                    |data_type |comment|
+----------------------------+----------+-------+
|c1                          |int       |null   |
|c2                          |string    |null   |
|                            |          |       |
|# Detailed Table Information|          |       |
|Database                    |tempdb1   |       |
|Table                       |v2        |       |
+----------------------------+----------+-------+

-- Before ALTER VIEW SET TBLPROPERTIES
DESC TABLE EXTENDED tempdb1.v2;

+----------------------------+----------+-------+
|col_name                    |data_type |comment|
+----------------------------+----------+-------+
|c1                          |int       |null   |
|c2                          |string    |null   |
|                            |          |       |
|# Detailed Table Information|          |       |
|Database                    |tempdb1   |       |
|Table                       |v2        |       |
|Table Properties            |[....]    |       |
+----------------------------+----------+-------+

-- Set properties in TBLPROPERTIES
ALTER VIEW tempdb1.v2 SET TBLPROPERTIES ('created.by.user' = "John", 'created.date' = '01-01-2001' );

-- Use `DESCRIBE TABLE EXTENDED tempdb1.v2` to verify
DESC TABLE EXTENDED tempdb1.v2;

+----------------------------+-----------------------------------------------------+-------+
|col_name                    |data_type                                            |comment|
+----------------------------+-----------------------------------------------------+-------+
|c1                          |int                                                  |null   |
|c2                          |string                                               |null   |
|                            |                                                     |       |
|# Detailed Table Information|                                                     |       |
|Database                    |tempdb1                                              |       |
|Table                       |v2                                                   |       |
|Table Properties            |[created.by.user=John, created.date=01-01-2001, ....]|       |
+----------------------------+-----------------------------------------------------+-------+

-- Remove the key `created.by.user` and `created.date` from `TBLPROPERTIES`
ALTER VIEW tempdb1.v2 UNSET TBLPROPERTIES ('created.by.user', 'created.date');

--Use `DESC TABLE EXTENDED tempdb1.v2` to verify the changes
DESC TABLE EXTENDED tempdb1.v2;

+----------------------------+----------+-------+
|col_name                    |data_type |comment|
+----------------------------+----------+-------+
|c1                          |int       |null   |
|c2                          |string    |null   |
|                            |          |       |
|# Detailed Table Information|          |       |
|Database                    |tempdb1   |       |
|Table                       |v2        |       |
|Table Properties            |[....]    |       |
+----------------------------+----------+-------+

-- Change the view definition
ALTER VIEW tempdb1.v2 AS SELECT * FROM tempdb1.v1;

-- Use `DESC TABLE EXTENDED` to verify
DESC TABLE EXTENDED tempdb1.v2;

+----------------------------+---------------------------+-------+
|col_name                    |data_type                  |comment|
+----------------------------+---------------------------+-------+
|c1                          |int                        |null   |
|c2                          |string                     |null   |
|                            |                           |       |
|# Detailed Table Information|                           |       |
|Database                    |tempdb1                    |       |
|Table                       |v2                         |       |
|Type                        |VIEW                       |       |
|View Text                   |select * from tempdb1.v1   |       |
|View Original Text          |select * from tempdb1.v1   |       |
+----------------------------+---------------------------+-------+
{% endhighlight %}

### Related Statements

- [describe-table](sql-ref-syntax-aux-describe-table.html)
- [create-view](sql-ref-syntax-ddl-create-view.html)
- [drop-view](sql-ref-syntax-ddl-drop-view.html)

#### Note:

`ALTER VIEW` statement does not support `SET SERDE` or `SET SERDEPROPERTIES` properties

