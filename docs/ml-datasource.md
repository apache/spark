---
layout: global
title: Data sources
displayTitle: Data sources
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

In this section, we introduce how to use data source in ML to load data.
Besides some general data sources such as Parquet, CSV, JSON and JDBC, we also provide some specific data sources for ML.

**Table of Contents**

* This will become a table of contents (this text will be scraped).
{:toc}

## Image data source

This image data source is used to load image files from a directory, it can load compressed image (jpeg, png, etc.) into raw image representation via `ImageIO` in Java library.
The loaded DataFrame has one `StructType` column: "image", containing image data stored as image schema.
The schema of the `image` column is:
 - origin: `StringType` (represents the file path of the image)
 - height: `IntegerType` (height of the image)
 - width: `IntegerType` (width of the image)
 - nChannels: `IntegerType` (number of image channels)
 - mode: `IntegerType` (OpenCV-compatible type)
 - data: `BinaryType` (Image bytes in OpenCV-compatible order: row-wise BGR in most cases)


<div class="codetabs">
<div data-lang="scala" markdown="1">
[`ImageDataSource`](api/scala/org/apache/spark/ml/source/image/ImageDataSource.html)
implements a Spark SQL data source API for loading image data as a DataFrame.

{% highlight scala %}
scala> val df = spark.read.format("image").option("dropInvalid", true).load("data/mllib/images/origin/kittens")
df: org.apache.spark.sql.DataFrame = [image: struct<origin: string, height: int ... 4 more fields>]

scala> df.select("image.origin", "image.width", "image.height").show(truncate=false)
+-----------------------------------------------------------------------+-----+------+
|origin                                                                 |width|height|
+-----------------------------------------------------------------------+-----+------+
|file:///spark/data/mllib/images/origin/kittens/54893.jpg               |300  |311   |
|file:///spark/data/mllib/images/origin/kittens/DP802813.jpg            |199  |313   |
|file:///spark/data/mllib/images/origin/kittens/29.5.a_b_EGDP022204.jpg |300  |200   |
|file:///spark/data/mllib/images/origin/kittens/DP153539.jpg            |300  |296   |
+-----------------------------------------------------------------------+-----+------+
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`ImageDataSource`](api/java/org/apache/spark/ml/source/image/ImageDataSource.html)
implements Spark SQL data source API for loading image data as a DataFrame.

{% highlight java %}
Dataset<Row> imagesDF = spark.read().format("image").option("dropInvalid", true).load("data/mllib/images/origin/kittens");
imageDF.select("image.origin", "image.width", "image.height").show(false);
/*
Will output:
+-----------------------------------------------------------------------+-----+------+
|origin                                                                 |width|height|
+-----------------------------------------------------------------------+-----+------+
|file:///spark/data/mllib/images/origin/kittens/54893.jpg               |300  |311   |
|file:///spark/data/mllib/images/origin/kittens/DP802813.jpg            |199  |313   |
|file:///spark/data/mllib/images/origin/kittens/29.5.a_b_EGDP022204.jpg |300  |200   |
|file:///spark/data/mllib/images/origin/kittens/DP153539.jpg            |300  |296   |
+-----------------------------------------------------------------------+-----+------+
*/
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
In PySpark we provide Spark SQL data source API for loading image data as a DataFrame.

{% highlight python %}
>>> df = spark.read.format("image").option("dropInvalid", True).load("data/mllib/images/origin/kittens")
>>> df.select("image.origin", "image.width", "image.height").show(truncate=False)
+-----------------------------------------------------------------------+-----+------+
|origin                                                                 |width|height|
+-----------------------------------------------------------------------+-----+------+
|file:///spark/data/mllib/images/origin/kittens/54893.jpg               |300  |311   |
|file:///spark/data/mllib/images/origin/kittens/DP802813.jpg            |199  |313   |
|file:///spark/data/mllib/images/origin/kittens/29.5.a_b_EGDP022204.jpg |300  |200   |
|file:///spark/data/mllib/images/origin/kittens/DP153539.jpg            |300  |296   |
+-----------------------------------------------------------------------+-----+------+
{% endhighlight %}
</div>

<div data-lang="r" markdown="1">
In SparkR we provide Spark SQL data source API for loading image data as a DataFrame.

{% highlight r %}
> df = read.df("data/mllib/images/origin/kittens", "image")
> head(select(df, df$image.origin, df$image.width, df$image.height))

1               file:///spark/data/mllib/images/origin/kittens/54893.jpg
2            file:///spark/data/mllib/images/origin/kittens/DP802813.jpg
3 file:///spark/data/mllib/images/origin/kittens/29.5.a_b_EGDP022204.jpg
4            file:///spark/data/mllib/images/origin/kittens/DP153539.jpg
  width height
1   300    311
2   199    313
3   300    200
4   300    296

{% endhighlight %}
</div>


</div>


## LIBSVM data source

This `LIBSVM` data source is used to load 'libsvm' type files from a directory.
The loaded DataFrame has two columns: label containing labels stored as doubles and features containing feature vectors stored as Vectors.
The schemas of the columns are:
 - label: `DoubleType` (represents the instance label)
 - features: `VectorUDT` (represents the feature vector)

<div class="codetabs">
<div data-lang="scala" markdown="1">
[`LibSVMDataSource`](api/scala/org/apache/spark/ml/source/libsvm/LibSVMDataSource.html)
implements a Spark SQL data source API for loading `LIBSVM` data as a DataFrame.

{% highlight scala %}
scala> val df = spark.read.format("libsvm").option("numFeatures", "780").load("data/mllib/sample_libsvm_data.txt")
df: org.apache.spark.sql.DataFrame = [label: double, features: vector]

scala> df.show(10)
+-----+--------------------+
|label|            features|
+-----+--------------------+
|  0.0|(780,[127,128,129...|
|  1.0|(780,[158,159,160...|
|  1.0|(780,[124,125,126...|
|  1.0|(780,[152,153,154...|
|  1.0|(780,[151,152,153...|
|  0.0|(780,[129,130,131...|
|  1.0|(780,[158,159,160...|
|  1.0|(780,[99,100,101,...|
|  0.0|(780,[154,155,156...|
|  0.0|(780,[127,128,129...|
+-----+--------------------+
only showing top 10 rows
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`LibSVMDataSource`](api/java/org/apache/spark/ml/source/libsvm/LibSVMDataSource.html)
implements Spark SQL data source API for loading `LIBSVM` data as a DataFrame.

{% highlight java %}
Dataset<Row> df = spark.read.format("libsvm").option("numFeatures", "780").load("data/mllib/sample_libsvm_data.txt");
df.show(10);
/*
Will output:
+-----+--------------------+
|label|            features|
+-----+--------------------+
|  0.0|(780,[127,128,129...|
|  1.0|(780,[158,159,160...|
|  1.0|(780,[124,125,126...|
|  1.0|(780,[152,153,154...|
|  1.0|(780,[151,152,153...|
|  0.0|(780,[129,130,131...|
|  1.0|(780,[158,159,160...|
|  1.0|(780,[99,100,101,...|
|  0.0|(780,[154,155,156...|
|  0.0|(780,[127,128,129...|
+-----+--------------------+
only showing top 10 rows
*/
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
In PySpark we provide Spark SQL data source API for loading `LIBSVM` data as a DataFrame.

{% highlight python %}
>>> df = spark.read.format("libsvm").option("numFeatures", "780").load("data/mllib/sample_libsvm_data.txt")
>>> df.show(10)
+-----+--------------------+
|label|            features|
+-----+--------------------+
|  0.0|(780,[127,128,129...|
|  1.0|(780,[158,159,160...|
|  1.0|(780,[124,125,126...|
|  1.0|(780,[152,153,154...|
|  1.0|(780,[151,152,153...|
|  0.0|(780,[129,130,131...|
|  1.0|(780,[158,159,160...|
|  1.0|(780,[99,100,101,...|
|  0.0|(780,[154,155,156...|
|  0.0|(780,[127,128,129...|
+-----+--------------------+
only showing top 10 rows
{% endhighlight %}
</div>

<div data-lang="r" markdown="1">
In SparkR we provide Spark SQL data source API for loading `LIBSVM` data as a DataFrame.

{% highlight r %}
> df = read.df("data/mllib/sample_libsvm_data.txt", "libsvm")
> head(select(df, df$label, df$features), 10)

   label                      features
1      0 <environment: 0x7fe6d35366e8>
2      1 <environment: 0x7fe6d353bf78>
3      1 <environment: 0x7fe6d3541840>
4      1 <environment: 0x7fe6d3545108>
5      1 <environment: 0x7fe6d354c8e0>
6      0 <environment: 0x7fe6d35501a8>
7      1 <environment: 0x7fe6d3555a70>
8      1 <environment: 0x7fe6d3559338>
9      0 <environment: 0x7fe6d355cc00>
10     0 <environment: 0x7fe6d35643d8>

{% endhighlight %}
</div>


</div>
