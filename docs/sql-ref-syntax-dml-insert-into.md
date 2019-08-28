---
layout: global
title: INSERT INTO
displayTitle: INSERT INTO
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

The `INSERT INTO` statement inserts new rows into a table. The inserted rows can be specified by value expressions or result from a query.

### Syntax
{% highlight sql %}
INSERT INTO [ TABLE ] table_name
    [ PARTITION ( partition_col_name [ = partition_col_val ] [ , ... ] ) ]
    { { VALUES ( { value | NULL } [ , ... ] ) [ , ( ... ) ] } | query }
{% endhighlight %}

### Parameters
#### ***table_name***:
The name of an existing table.

#### ***PARTITION ( partition_col_name [ = partition_col_val ] [ , ... ] )***:
Specifies one or more partition column and value pairs. The partition value is optional.

#### ***VALUES ( { value | NULL } [ , ... ] ) [ , ( ... ) ]***:
Specifies the values to be inserted. Either an explicitly specified value or a NULL can be inserted. A comma must be used to seperate each value in the clause. More than one set of values can be specified to insert multiple rows.

#### ***query***:
A query that produces the rows to be inserted. It can be in one of following formats:
- a `SELECT` statement
- a `TABLE` statement
- a `FROM` statement

### Examples
#### Single Row Insert Using a VALUES Clause
{% highlight sql %}
 CREATE TABLE students (Name VARCHAR(64), Address VARCHAR(64), StudentID INT)
     USING PARQUET PARTITIONED BY (StudentID);

 INSERT INTO students
     VALUES ('Amy Smith', '123 Park Ave, San Jose', 111111);

 SELECT * FROM students;

     + -------------- + ------------------------------ + -------------- +
     | Name           | Address                        | StudentID      |
     + -------------- + ------------------------------ + -------------- +
     | Amy Smith      | 123 Park Ave, San Jose         | 111111         |
     + -------------- + ------------------------------ + -------------- +
{% endhighlight %}

#### Multi-Row Insert Using a VALUES Clause
{% highlight sql %}
 INSERT INTO students
     VALUES ('Bob Brown', '456 Taylor St, Cupertino', 222222),
            ('Cathy Johnson', '789 Race Ave, Palo Alto', 333333);

 SELECT * FROM students;

     + -------------- + ------------------------------ + -------------- +
     | Name           | Address                        | StudentID      |
     + -------------- + ------------------------------ + -------------- +
     | Amy Smith      | 123 Park Ave, San Jose         | 111111         |
     + -------------- + ------------------------------ + -------------- +
     | Bob Brown      | 456 Taylor St, Cupertino       | 222222         |
     + -------------- + ------------------------------ + -------------- +
     | Cathy Johnson  | 789 Race Ave, Palo Alto        | 333333         |
     + -------------- + ------------------------------ + -------------- +
{% endhighlight %}

#### Insert Using a SELECT Statement
Assuming the `persons` table has already been created and populated.

{% highlight sql %}
 SELECT * FROM persons;

     + -------------- + ------------------------------ + -------------- +
     | Name           | Address                        | SSN            |
     + -------------- + ------------------------------ + -------------- +
     | Dora Williams  | 134 Forest Ave, Melo Park      | 123456789      |
     + -------------- + ------------------------------ + -------------- +
     | Eddie Davis    | 245 Market St, Milpitas        | 345678901      |
     + -------------- + ------------------------------ + ---------------+

 INSERT INTO students PARTITION (StudentID = 444444)
     SELECT Name, Address FROM persons WHERE name = "Dora Williams";

 SELECT * FROM students;

     + -------------- + ------------------------------ + -------------- +
     | Name           | Address                        | StudentID      |
     + -------------- + ------------------------------ + -------------- +
     | Amy Smith      | 123 Park Ave, San Jose         | 111111         |
     + -------------- + ------------------------------ + -------------- +
     | Bob Brown      | 456 Taylor St, Cupertino       | 222222         |
     + -------------- + ------------------------------ + -------------- +
     | Cathy Johnson  | 789 Race Ave, Palo Alto        | 333333         |
     + -------------- + ------------------------------ + -------------- +
     | Dora Williams  | 134 Forest Ave, Melo Park      | 444444         |
     + -------------- + ------------------------------ + -------------- +
{% endhighlight %}

#### Insert Using a TABLE Statement
Assuming the `visitingStudents` table has already been created and populated.

{% highlight sql %}
 SELECT * FROM visitingStudents;

     + -------------- + ------------------------------ + -------------- +
     | Name           | Address                        | StudentID      |
     + -------------- + ------------------------------ + -------------- +
     | Fleur Laurent  | 345 Copper St, London          | 777777         |
     + -------------- + ------------------------------ + -------------- +
     | Gordon Martin  | 779 Lake Ave, Oxford           | 888888         |
     + -------------- + ------------------------------ + -------------- +

 INSERT INTO students TABLE visitingStudents;

 SELECT * FROM students;

     + -------------- + ------------------------------ + -------------- +
     | Name           | Address                        | StudentID      |
     + -------------- + ------------------------------ + -------------- +
     | Amy Smith      | 123 Park Ave, San Jose         | 111111         |
     + -------------- + ------------------------------ + -------------- +
     | Bob Brown      | 456 Taylor St, Cupertino       | 222222         |
     + -------------- + ------------------------------ + -------------- +
     | Cathy Johnson  | 789 Race Ave, Palo Alto        | 333333         |
     + -------------- + ------------------------------ + -------------- +
     | Dora Williams  | 134 Forest Ave, Melo Park      | 444444         |
     + -------------- + ------------------------------ + -------------- +
     | Fleur Laurent  | 345 Copper St, London          | 777777         |
     + -------------- + ------------------------------ + -------------- +
     | Gordon Martin  | 779 Lake Ave, Oxford           | 888888         |
     + -------------- + ------------------------------ + -------------- +
{% endhighlight %}

#### Insert Using a FROM Statement
Assuming the `applicants` table has already been created and populated.

{% highlight sql %}
 SELECT * FROM applicants;

     + -------------- + ------------------------------ + -------------- + -------------- +
     | Name           | Address                        | StudentID      | Qualified      |
     + -------------- + ------------------------------ + -------------- + -------------- +
     | Helen Davis    | 469 Mission St, San Diego      | 999999         | true           |
     + -------------- + ------------------------------ + -------------- + -------------- +
     | Ivy King       | 367 Leigh Ave, Santa Clara     | 101010         | false          |
     + -------------- + ------------------------------ + -------------- + -------------- +
     | Jason Wang     | 908 Bird St, Saratoga          | 121212         | true           |
     + -------------- + ------------------------------ + -------------- + -------------- +

 INSERT INTO students
      FROM applicants SELECT Name, Address, ID applicants WHERE Qualified = TRUE;

 SELECT * FROM students;

     + -------------- + ------------------------------ + -------------- +
     | Name           | Address                        | StudentID      |
     + -------------- + ------------------------------ + -------------- +
     | Amy Smith      | 123 Park Ave, San Jose         | 111111         |
     + -------------- + ------------------------------ + -------------- +
     | Bob Brown      | 456 Taylor St, Cupertino       | 222222         |
     + -------------- + ------------------------------ + -------------- +
     | Cathy Johnson  | 789 Race Ave, Palo Alto        | 333333         |
     + -------------- + ------------------------------ + -------------- +
     | Dora Williams  | 134 Forest Ave, Melo Park      | 444444         |
     + -------------- + ------------------------------ + -------------- +
     | Fleur Laurent  | 345 Copper St, London          | 777777         |
     + -------------- + ------------------------------ + -------------- +
     | Gordon Martin  | 779 Lake Ave, Oxford           | 888888         |
     + -------------- + ------------------------------ + -------------- +
     | Helen Davis    | 469 Mission St, San Diego      | 999999         |
     + -------------- + ------------------------------ + -------------- +
     | Jason Wang     | 908 Bird St, Saratoga          | 121212         |
     + -------------- + ------------------------------ + -------------- +
{% endhighlight %}

Related Statements:
  * [INSERT OVERWRITE statement](sql-ref-syntax-dml-insert-overwrite-table.html)
  * [INSERT OVERWRITE DIRECTORY statement](sql-ref-syntax-dml-insert-overwrite-directory.html)
  * [INSERT OVERWRITE DIRECTORY with Hive format statement](sql-ref-syntax-dml-insert-overwrite-directory-hive.html)