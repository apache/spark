---
layout: global
title: Data sources
displayTitle: Data sources
---

In this section, we introduce how to use data source in ML to load data.
Beside some general data sources like Parquet, CSV, JSON, JDBC, we also provide some specific data source for ML.

**Table of Contents**

* This will become a table of contents (this text will be scraped).
{:toc}

## Image data source

This image data source is used to load image files from a directory.
The loaded DataFrame has one StructType column: "image". containing image data stored as image schema.

<div class="codetabs">
<div data-lang="scala" markdown="1">
[`ImageDataSource`](api/scala/index.html#org.apache.spark.ml.source.image.ImageDataSource)
implements a Spark SQL data source API for loading image data as a DataFrame.

{% highlight scala %}
scala> spark.read.format("image").load("data/mllib/images/origin")
res1: org.apache.spark.sql.DataFrame = [image: struct<origin: string, height: int ... 4 more fields>]
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`ImageDataSource`](api/java/org/apache/spark/ml/source/image/ImageDataSource.html)
implements Spark SQL data source API for loading image data as DataFrame.

{% highlight java %}
Dataset<Row> imagesDF = spark.read().format("image").load("data/mllib/images/origin");
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
In PySpark we provide Spark SQL data source API for loading image data as DataFrame.

{% highlight python %}
>>> spark.read.format("image").load("data/mllib/images/origin")
DataFrame[image: struct<origin:string,height:int,width:int,nChannels:int,mode:int,data:binary>]
{% endhighlight %}
</div>

</div>
