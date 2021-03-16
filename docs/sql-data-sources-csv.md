---
layout: global
title: CSV Files
displayTitle: CSV Files
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

<div class="codetabs">

<div data-lang="scala"  markdown="1">

Spark SQL provides `spark.read.csv("file_name")` to read a CSV file into Spark DataFrame and `dataframe.write.csv("path")` to save or write to the CSV file. Spark supports reading pipe, comma, tab, or any other delimiter/seperator files.

{% highlight scala %}
import spark.implicits._
val path = "src/main/resources/people.csv"

val df = spark.read.csv(path)
df.show()
// +------------------+
// |               _c0|
// +------------------+
// |      name;age;job|
// |Jorge;30;Developer|
// |  Bob;32;Developer|
// +------------------+

// Read a csv with delimiter
val df2 = spark.read.options(Map("delimiter"->";")).csv(path)
df2.show()
// +-----+---+---------+
// |  _c0|_c1|      _c2|
// +-----+---+---------+
// | name|age|      job|
// |Jorge| 30|Developer|
// |  Bob| 32|Developer|
// +-----+---+---------+

// Read a csv with delimiter and a header
val df3 = spark.read.options(Map("delimiter"->";","header"->"true")).csv(path)
df3.show()
// +-----+---+---------+
// | name|age|      job|
// +-----+---+---------+
// |Jorge| 30|Developer|
// |  Bob| 32|Developer|
// +-----+---+---------+

df3.write.csv("output")
// "output" is a folder which contains multiple csv files and a _SUCCESS file.

{% endhighlight %}

</div>

</div>
