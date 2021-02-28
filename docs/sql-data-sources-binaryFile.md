---
layout: global
title: Binary File Data Source
displayTitle: Binary File Data Source
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

Since Spark 3.0, Spark supports binary file data source,
which reads binary files and converts each file into a single record that contains the raw content
and metadata of the file.
It produces a DataFrame with the following columns and possibly partition columns:
* `path`: StringType
* `modificationTime`: TimestampType
* `length`: LongType
* `content`: BinaryType

To read whole binary files, you need to specify the data source `format` as `binaryFile`.
To load files with paths matching a given glob pattern while keeping the behavior of partition discovery,
you can use the general data source option `pathGlobFilter`.
For example, the following code reads all PNG files from the input directory:

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}

spark.read.format("binaryFile").option("pathGlobFilter", "*.png").load("/path/to/data")

{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}

spark.read().format("binaryFile").option("pathGlobFilter", "*.png").load("/path/to/data");

{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}

spark.read.format("binaryFile").option("pathGlobFilter", "*.png").load("/path/to/data")

{% endhighlight %}
</div>
<div data-lang="r" markdown="1">
{% highlight r %}

read.df("/path/to/data", source = "binaryFile", pathGlobFilter = "*.png")

{% endhighlight %}
</div>
</div>

Binary file data source does not support writing a DataFrame back to the original files.
