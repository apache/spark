---
layout: global
title: Quick Start
description: Quick start tutorial for Spark SPARK_VERSION_SHORT
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

* This will become a table of contents (this text will be scraped).
{:toc}

This tutorial provides a quick introduction to using Spark. We will first introduce the API through Spark's
interactive shell (in Python or Scala),
then show how to write applications in Java, Scala, and Python.

To follow along with this guide, first, download a packaged release of Spark from the
[Spark website](https://spark.apache.org/downloads.html). Since we won't be using HDFS,
you can download a package for any version of Hadoop.

Note that, before Spark 2.0, the main programming interface of Spark was the Resilient Distributed Dataset (RDD). After Spark 2.0, RDDs are replaced by Dataset, which is strongly-typed like an RDD, but with richer optimizations under the hood. The RDD interface is still supported, and you can get a more detailed reference at the [RDD programming guide](rdd-programming-guide.html). However, we highly recommend you to switch to use Dataset, which has better performance than RDD. See the [SQL programming guide](sql-programming-guide.html) to get more information about Dataset.

# Interactive Analysis with the Spark Shell

## Basics

Spark's shell provides a simple way to learn the API, as well as a powerful tool to analyze data interactively.
It is available in either Scala (which runs on the Java VM and is thus a good way to use existing Java libraries)
or Python. Start it by running the following in the Spark directory:

<div class="codetabs">
<div data-lang="scala" markdown="1">

    ./bin/spark-shell

Spark's primary abstraction is a distributed collection of items called a Dataset. Datasets can be created from Hadoop InputFormats (such as HDFS files) or by transforming other Datasets. Let's make a new Dataset from the text of the README file in the Spark source directory:

{% highlight scala %}
scala> val textFile = spark.read.textFile("README.md")
textFile: org.apache.spark.sql.Dataset[String] = [value: string]
{% endhighlight %}

You can get values from Dataset directly, by calling some actions, or transform the Dataset to get a new one. For more details, please read the _[API doc](api/scala/org/apache/spark/sql/Dataset.html)_.

{% highlight scala %}
scala> textFile.count() // Number of items in this Dataset
res0: Long = 126 // May be different from yours as README.md will change over time, similar to other outputs

scala> textFile.first() // First item in this Dataset
res1: String = # Apache Spark
{% endhighlight %}

Now let's transform this Dataset into a new one. We call `filter` to return a new Dataset with a subset of the items in the file.

{% highlight scala %}
scala> val linesWithSpark = textFile.filter(line => line.contains("Spark"))
linesWithSpark: org.apache.spark.sql.Dataset[String] = [value: string]
{% endhighlight %}

We can chain together transformations and actions:

{% highlight scala %}
scala> textFile.filter(line => line.contains("Spark")).count() // How many lines contain "Spark"?
res3: Long = 15
{% endhighlight %}

</div>
<div data-lang="python" markdown="1">

    ./bin/pyspark


Or if PySpark is installed with pip in your current environment:

    pyspark

Spark's primary abstraction is a distributed collection of items called a Dataset. Datasets can be created from Hadoop InputFormats (such as HDFS files) or by transforming other Datasets. Due to Python's dynamic nature, we don't need the Dataset to be strongly-typed in Python. As a result, all Datasets in Python are Dataset[Row], and we call it `DataFrame` to be consistent with the data frame concept in Pandas and R. Let's make a new DataFrame from the text of the README file in the Spark source directory:

{% highlight python %}
>>> textFile = spark.read.text("README.md")
{% endhighlight %}

You can get values from DataFrame directly, by calling some actions, or transform the DataFrame to get a new one. For more details, please read the _[API doc](api/python/index.html#pyspark.sql.DataFrame)_.

{% highlight python %}
>>> textFile.count()  # Number of rows in this DataFrame
126

>>> textFile.first()  # First row in this DataFrame
Row(value=u'# Apache Spark')
{% endhighlight %}

Now let's transform this DataFrame to a new one. We call `filter` to return a new DataFrame with a subset of the lines in the file.

{% highlight python %}
>>> linesWithSpark = textFile.filter(textFile.value.contains("Spark"))
{% endhighlight %}

We can chain together transformations and actions:

{% highlight python %}
>>> textFile.filter(textFile.value.contains("Spark")).count()  # How many lines contain "Spark"?
15
{% endhighlight %}

</div>
</div>


## More on Dataset Operations
Dataset actions and transformations can be used for more complex computations. Let's say we want to find the line with the most words:

<div class="codetabs">
<div data-lang="scala" markdown="1">

{% highlight scala %}
scala> textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
res4: Int = 15
{% endhighlight %}

This first maps a line to an integer value, creating a new Dataset. `reduce` is called on that Dataset to find the largest word count. The arguments to `map` and `reduce` are Scala function literals (closures), and can use any language feature or Scala/Java library. For example, we can easily call functions declared elsewhere. We'll use `Math.max()` function to make this code easier to understand:

{% highlight scala %}
scala> import java.lang.Math
import java.lang.Math

scala> textFile.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b))
res5: Int = 15
{% endhighlight %}

One common data flow pattern is MapReduce, as popularized by Hadoop. Spark can implement MapReduce flows easily:

{% highlight scala %}
scala> val wordCounts = textFile.flatMap(line => line.split(" ")).groupByKey(identity).count()
wordCounts: org.apache.spark.sql.Dataset[(String, Long)] = [value: string, count(1): bigint]
{% endhighlight %}

Here, we call `flatMap` to transform a Dataset of lines to a Dataset of words, and then combine `groupByKey` and `count` to compute the per-word counts in the file as a Dataset of (String, Long) pairs. To collect the word counts in our shell, we can call `collect`:

{% highlight scala %}
scala> wordCounts.collect()
res6: Array[(String, Int)] = Array((means,1), (under,2), (this,3), (Because,1), (Python,2), (agree,1), (cluster.,1), ...)
{% endhighlight %}

</div>
<div data-lang="python" markdown="1">

{% highlight python %}
>>> from pyspark.sql.functions import *
>>> textFile.select(size(split(textFile.value, "\s+")).name("numWords")).agg(max(col("numWords"))).collect()
[Row(max(numWords)=15)]
{% endhighlight %}

This first maps a line to an integer value and aliases it as "numWords", creating a new DataFrame. `agg` is called on that DataFrame to find the largest word count. The arguments to `select` and `agg` are both _[Column](api/python/index.html#pyspark.sql.Column)_, we can use `df.colName` to get a column from a DataFrame. We can also import pyspark.sql.functions, which provides a lot of convenient functions to build a new Column from an old one.

One common data flow pattern is MapReduce, as popularized by Hadoop. Spark can implement MapReduce flows easily:

{% highlight python %}
>>> wordCounts = textFile.select(explode(split(textFile.value, "\s+")).alias("word")).groupBy("word").count()
{% endhighlight %}

Here, we use the `explode` function in `select`, to transform a Dataset of lines to a Dataset of words, and then combine `groupBy` and `count` to compute the per-word counts in the file as a DataFrame of 2 columns: "word" and "count". To collect the word counts in our shell, we can call `collect`:

{% highlight python %}
>>> wordCounts.collect()
[Row(word=u'online', count=1), Row(word=u'graphs', count=1), ...]
{% endhighlight %}

</div>
</div>

## Caching
Spark also supports pulling data sets into a cluster-wide in-memory cache. This is very useful when data is accessed repeatedly, such as when querying a small "hot" dataset or when running an iterative algorithm like PageRank. As a simple example, let's mark our `linesWithSpark` dataset to be cached:

<div class="codetabs">
<div data-lang="scala" markdown="1">

{% highlight scala %}
scala> linesWithSpark.cache()
res7: linesWithSpark.type = [value: string]

scala> linesWithSpark.count()
res8: Long = 15

scala> linesWithSpark.count()
res9: Long = 15
{% endhighlight %}

It may seem silly to use Spark to explore and cache a 100-line text file. The interesting part is
that these same functions can be used on very large data sets, even when they are striped across
tens or hundreds of nodes. You can also do this interactively by connecting `bin/spark-shell` to
a cluster, as described in the [RDD programming guide](rdd-programming-guide.html#using-the-shell).

</div>
<div data-lang="python" markdown="1">

{% highlight python %}
>>> linesWithSpark.cache()

>>> linesWithSpark.count()
15

>>> linesWithSpark.count()
15
{% endhighlight %}

It may seem silly to use Spark to explore and cache a 100-line text file. The interesting part is
that these same functions can be used on very large data sets, even when they are striped across
tens or hundreds of nodes. You can also do this interactively by connecting `bin/pyspark` to
a cluster, as described in the [RDD programming guide](rdd-programming-guide.html#using-the-shell).

</div>
</div>

# Self-Contained Applications
Suppose we wish to write a self-contained application using the Spark API. We will walk through a
simple application in Scala (with sbt), Java (with Maven), and Python (pip).

<div class="codetabs">
<div data-lang="scala" markdown="1">

We'll create a very simple Spark application in Scala--so simple, in fact, that it's
named `SimpleApp.scala`:

{% highlight scala %}
/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
{% endhighlight %}

Note that applications should define a `main()` method instead of extending `scala.App`.
Subclasses of `scala.App` may not work correctly.

This program just counts the number of lines containing 'a' and the number containing 'b' in the
Spark README. Note that you'll need to replace YOUR_SPARK_HOME with the location where Spark is
installed. Unlike the earlier examples with the Spark shell, which initializes its own SparkSession,
we initialize a SparkSession as part of the program.

We call `SparkSession.builder` to construct a `SparkSession`, then set the application name, and finally call `getOrCreate` to get the `SparkSession` instance.

Our application depends on the Spark API, so we'll also include an sbt configuration file,
`build.sbt`, which explains that Spark is a dependency. This file also adds a repository that
Spark depends on:

{% highlight scala %}
name := "Simple Project"

version := "1.0"

scalaVersion := "{{site.SCALA_VERSION}}"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "{{site.SPARK_VERSION}}"
{% endhighlight %}

For sbt to work correctly, we'll need to layout `SimpleApp.scala` and `build.sbt`
according to the typical directory structure. Once that is in place, we can create a JAR package
containing the application's code, then use the `spark-submit` script to run our program.

{% highlight bash %}
# Your directory layout should look like this
$ find .
.
./build.sbt
./src
./src/main
./src/main/scala
./src/main/scala/SimpleApp.scala

# Package a jar containing your application
$ sbt package
...
[info] Packaging {..}/{..}/target/scala-{{site.SCALA_BINARY_VERSION}}/simple-project_{{site.SCALA_BINARY_VERSION}}-1.0.jar

# Use spark-submit to run your application
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class "SimpleApp" \
  --master local[4] \
  target/scala-{{site.SCALA_BINARY_VERSION}}/simple-project_{{site.SCALA_BINARY_VERSION}}-1.0.jar
...
Lines with a: 46, Lines with b: 23
{% endhighlight %}

</div>
<div data-lang="java" markdown="1">
This example will use Maven to compile an application JAR, but any similar build system will work.

We'll create a very simple Spark application, `SimpleApp.java`:

{% highlight java %}
/* SimpleApp.java */
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SimpleApp {
  public static void main(String[] args) {
    String logFile = "YOUR_SPARK_HOME/README.md"; // Should be some file on your system
    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
    Dataset<String> logData = spark.read().textFile(logFile).cache();

    long numAs = logData.filter(s -> s.contains("a")).count();
    long numBs = logData.filter(s -> s.contains("b")).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

    spark.stop();
  }
}
{% endhighlight %}

This program just counts the number of lines containing 'a' and the number containing 'b' in the
Spark README. Note that you'll need to replace YOUR_SPARK_HOME with the location where Spark is
installed. Unlike the earlier examples with the Spark shell, which initializes its own SparkSession,
we initialize a SparkSession as part of the program.

To build the program, we also write a Maven `pom.xml` file that lists Spark as a dependency.
Note that Spark artifacts are tagged with a Scala version.

{% highlight xml %}
<project>
  <groupId>edu.berkeley</groupId>
  <artifactId>simple-project</artifactId>
  <modelVersion>4.0.0</modelVersion>
  <name>Simple Project</name>
  <packaging>jar</packaging>
  <version>1.0</version>
  <dependencies>
    <dependency> <!-- Spark dependency -->
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-sql_{{site.SCALA_BINARY_VERSION}}</artifactId>
      <version>{{site.SPARK_VERSION}}</version>
      <scope>provided</scope>
    </dependency>
  </dependencies>
</project>
{% endhighlight %}

We lay out these files according to the canonical Maven directory structure:
{% highlight bash %}
$ find .
./pom.xml
./src
./src/main
./src/main/java
./src/main/java/SimpleApp.java
{% endhighlight %}

Now, we can package the application using Maven and execute it with `./bin/spark-submit`.

{% highlight bash %}
# Package a JAR containing your application
$ mvn package
...
[INFO] Building jar: {..}/{..}/target/simple-project-1.0.jar

# Use spark-submit to run your application
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class "SimpleApp" \
  --master local[4] \
  target/simple-project-1.0.jar
...
Lines with a: 46, Lines with b: 23
{% endhighlight %}

</div>
<div data-lang="python" markdown="1">

Now we will show how to write an application using the Python API (PySpark).


If you are building a packaged PySpark application or library you can add it to your setup.py file as:

{% highlight python %}
    install_requires=[
        'pyspark=={{site.SPARK_VERSION}}'
    ]
{% endhighlight %}


As an example, we'll create a simple Spark application, `SimpleApp.py`:

{% highlight python %}
"""SimpleApp.py"""
from pyspark.sql import SparkSession

logFile = "YOUR_SPARK_HOME/README.md"  # Should be some file on your system
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
{% endhighlight %}


This program just counts the number of lines containing 'a' and the number containing 'b' in a
text file.
Note that you'll need to replace YOUR_SPARK_HOME with the location where Spark is installed.
As with the Scala and Java examples, we use a SparkSession to create Datasets.
For applications that use custom classes or third-party libraries, we can also add code
dependencies to `spark-submit` through its `--py-files` argument by packaging them into a
.zip file (see `spark-submit --help` for details).
`SimpleApp` is simple enough that we do not need to specify any code dependencies.

We can run this application using the `bin/spark-submit` script:

{% highlight bash %}
# Use spark-submit to run your application
$ YOUR_SPARK_HOME/bin/spark-submit \
  --master local[4] \
  SimpleApp.py
...
Lines with a: 46, Lines with b: 23
{% endhighlight %}

If you have PySpark pip installed into your environment (e.g., `pip install pyspark`), you can run your application with the regular Python interpreter or use the provided 'spark-submit' as you prefer.

{% highlight bash %}
# Use the Python interpreter to run your application
$ python SimpleApp.py
...
Lines with a: 46, Lines with b: 23
{% endhighlight %}

</div>
</div>

Other dependency management tools such as Conda and pip can be also used for custom classes or third-party libraries. See also [Python Package Management](api/python/user_guide/python_packaging.html).

# Where to Go from Here
Congratulations on running your first Spark application!

* For an in-depth overview of the API, start with the [RDD programming guide](rdd-programming-guide.html) and the [SQL programming guide](sql-programming-guide.html), or see "Programming Guides" menu for other components.
* For running applications on a cluster, head to the [deployment overview](cluster-overview.html).
* Finally, Spark includes several samples in the `examples` directory
([Scala]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/scala/org/apache/spark/examples),
 [Java]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/java/org/apache/spark/examples),
 [Python]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/python),
 [R]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/r)).
You can run them as follows:

{% highlight bash %}
# For Scala and Java, use run-example:
./bin/run-example SparkPi

# For Python examples, use spark-submit directly:
./bin/spark-submit examples/src/main/python/pi.py

# For R examples, use spark-submit directly:
./bin/spark-submit examples/src/main/r/dataframe.R
{% endhighlight %}
