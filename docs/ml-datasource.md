---
layout: global
title: Data sources
displayTitle: Data sources
---

In this section, we introduce how to use data source in ML to load data.
Beside some general data sources such as Parquet, CSV, JSON and JDBC, we also provide some specific data sources for ML.

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
[`ImageDataSource`](api/scala/index.html#org.apache.spark.ml.source.image.ImageDataSource)
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
implements Spark SQL data source API for loading image data as DataFrame.

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
In PySpark we provide Spark SQL data source API for loading image data as DataFrame.

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
In SparkR we provide Spark SQL data source API for loading image data as DataFrame.

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
