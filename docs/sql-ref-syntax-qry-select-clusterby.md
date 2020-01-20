---
layout: global
title: CLUSTER BY Clause
displayTitle: CLUSTER BY Clause
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
The <code>CLUSTER BY</code> clause is used to first repartition the data based
on the input expressions and then sort the data within each partition. This is
is semantically equivalent to performing <code>DISTRIBUTE BY</code> followed by
<code>SORT BY</code>. This cause only ensures that the resultant rows are
sorted within each partition and does not guarantee total ordering of output,

### Syntax
{% highlight sql %}
CLUSTER BY { expression [ , ...] }
{% endhighlight %}

### Parameters
<dl>
  <dt><code><em>expression</em></code></dt>
  <dd>
    Specifies combination of one or more values, operators and SQL functions that results in a value.
  </dd>
</dl>

### Examples
{% highlight sql %}
CREATE TABLE person (name STRING, age INT);
INSERT INTO person VALUES ('Zen Hui', 25), 
                          ('Anil B', 18), 
                          ('Shone S', 16), 
                          ('Mike A', 25),
                          ('John A', 18), 
                          ('Jack N', 16);
-- Reduce the number of shuffle partitions to 2 to illustrate the behaviour of `CLUSTER BY`.
-- Its easier to see the clustering and sorting behaviour with less number of partitions.
SET spark.sql.shuffle.partitions = 2;
                        
-- Select the rows with no ordering. Please note that without any sort directive, the results
-- of the query is not deterministic. Its included here to show the difference in behaviour 
-- of a query when `CLUSTER BY` is not used vs when its used. The query below produces rows
-- where age column is not sorted.
SELECT age, name FROM person;

  +---+-------+
  |age|name   |
  +---+-------+
  |16 |Shone S|
  |25 |Zen Hui|
  |16 |Jack N |
  |25 |Mike A |
  |18 |John A |
  |18 |Anil B |
  +---+-------+

-- Produces rows clustered by age. Persons with same age are clustered together.
-- In the query below, persons with age 18 and 25 are in first partition and the
-- persons with age 16 are in the second partition. The rows are sorted based
-- on age within each partition.
SELECT age, name FROM person CLUSTER BY age;

  +---+-------+
  |age|name   |
  +---+-------+
  |18 |John A |
  |18 |Anil B |
  |25 |Zen Hui|
  |25 |Mike A |
  |16 |Shone S|
  |16 |Jack N |
  +---+-------+
{% endhighlight %}
