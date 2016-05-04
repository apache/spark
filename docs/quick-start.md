---
layout: global
title: Quick Start
description: Quick start tutorial for Spark SPARK_VERSION_SHORT
---

* This will become a table of contents (this text will be scraped).
{:toc}

This tutorial provides a quick introduction to using Spark. We will first introduce the API through Spark's
interactive shell (in Python or Scala),
then show how to write applications in Java, Scala, and Python.
See the [programming guide](programming-guide.html) for a more complete reference.

To follow along with this guide, first download a packaged release of Spark from the
[Spark website](http://spark.apache.org/downloads.html). Since we won't be using HDFS,
you can download a package for any version of Hadoop.

# Interactive Analysis with the Spark Shell

## Basics

Spark's shell provides a simple way to learn the API, as well as a powerful tool to analyze data interactively.
It is available in either Scala (which runs on the Java VM and is thus a good way to use existing Java libraries)
or Python. Start it by running the following in the Spark directory:

<div class="codetabs">
<div data-lang="scala" markdown="1">

    ./bin/spark-shell

Spark's primary abstraction is a distributed collection of items called a Resilient Distributed Dataset (RDD). RDDs can be created from Hadoop InputFormats (such as HDFS files) or by transforming other RDDs. Let's make a new RDD from the text of the README file in the Spark source directory:

{% highlight scala %}
scala> val textFile = sc.textFile("README.md")
textFile: org.apache.spark.rdd.RDD[String] = README.md MapPartitionsRDD[1] at textFile at <console>:25
{% endhighlight %}

RDDs have _[actions](programming-guide.html#actions)_, which return values, and _[transformations](programming-guide.html#transformations)_, which return pointers to new RDDs. Let's start with a few actions:

{% highlight scala %}
scala> textFile.count() // Number of items in this RDD
res0: Long = 126

scala> textFile.first() // First item in this RDD
res1: String = # Apache Spark
{% endhighlight %}

Now let's use a transformation. We will use the [`filter`](programming-guide.html#transformations) transformation to return a new RDD with a subset of the items in the file.

{% highlight scala %}
scala> val linesWithSpark = textFile.filter(line => line.contains("Spark"))
linesWithSpark: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[2] at filter at <console>:27
{% endhighlight %}

We can chain together transformations and actions:

{% highlight scala %}
scala> textFile.filter(line => line.contains("Spark")).count() // How many lines contain "Spark"?
res3: Long = 15
{% endhighlight %}

</div>
<div data-lang="python" markdown="1">

    ./bin/pyspark

Spark's primary abstraction is a distributed collection of items called a Resilient Distributed Dataset (RDD). RDDs can be created from Hadoop InputFormats (such as HDFS files) or by transforming other RDDs. Let's make a new RDD from the text of the README file in the Spark source directory:

{% highlight python %}
>>> textFile = sc.textFile("README.md")
{% endhighlight %}

RDDs have _[actions](programming-guide.html#actions)_, which return values, and _[transformations](programming-guide.html#transformations)_, which return pointers to new RDDs. Let's start with a few actions:

{% highlight python %}
>>> textFile.count() # Number of items in this RDD
126

>>> textFile.first() # First item in this RDD
u'# Apache Spark'
{% endhighlight %}

Now let's use a transformation. We will use the [`filter`](programming-guide.html#transformations) transformation to return a new RDD with a subset of the items in the file.

{% highlight python %}
>>> linesWithSpark = textFile.filter(lambda line: "Spark" in line)
{% endhighlight %}

We can chain together transformations and actions:

{% highlight python %}
>>> textFile.filter(lambda line: "Spark" in line).count() # How many lines contain "Spark"?
15
{% endhighlight %}

</div>
</div>


## More on RDD Operations
RDD actions and transformations can be used for more complex computations. Let's say we want to find the line with the most words:

<div class="codetabs">
<div data-lang="scala" markdown="1">

{% highlight scala %}
scala> textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
res4: Long = 15
{% endhighlight %}

This first maps a line to an integer value, creating a new RDD. `reduce` is called on that RDD to find the largest line count. The arguments to `map` and `reduce` are Scala function literals (closures), and can use any language feature or Scala/Java library. For example, we can easily call functions declared elsewhere. We'll use `Math.max()` function to make this code easier to understand:

{% highlight scala %}
scala> import java.lang.Math
import java.lang.Math

scala> textFile.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b))
res5: Int = 15
{% endhighlight %}

One common data flow pattern is MapReduce, as popularized by Hadoop. Spark can implement MapReduce flows easily:

{% highlight scala %}
scala> val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
wordCounts: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[8] at reduceByKey at <console>:28
{% endhighlight %}

Here, we combined the [`flatMap`](programming-guide.html#transformations), [`map`](programming-guide.html#transformations), and [`reduceByKey`](programming-guide.html#transformations) transformations to compute the per-word counts in the file as an RDD of (String, Int) pairs. To collect the word counts in our shell, we can use the [`collect`](programming-guide.html#actions) action:

{% highlight scala %}
scala> wordCounts.collect()
res6: Array[(String, Int)] = Array((means,1), (under,2), (this,3), (Because,1), (Python,2), (agree,1), (cluster.,1), ...)
{% endhighlight %}

</div>
<div data-lang="python" markdown="1">

{% highlight python %}
>>> textFile.map(lambda line: len(line.split())).reduce(lambda a, b: a if (a > b) else b)
15
{% endhighlight %}

This first maps a line to an integer value, creating a new RDD. `reduce` is called on that RDD to find the largest line count. The arguments to `map` and `reduce` are Python [anonymous functions (lambdas)](https://docs.python.org/2/reference/expressions.html#lambda),
but we can also pass any top-level Python function we want.
For example, we'll define a `max` function to make this code easier to understand:

{% highlight python %}
>>> def max(a, b):
...     if a > b:
...         return a
...     else:
...         return b
...

>>> textFile.map(lambda line: len(line.split())).reduce(max)
15
{% endhighlight %}

One common data flow pattern is MapReduce, as popularized by Hadoop. Spark can implement MapReduce flows easily:

{% highlight python %}
>>> wordCounts = textFile.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
{% endhighlight %}

Here, we combined the [`flatMap`](programming-guide.html#transformations), [`map`](programming-guide.html#transformations), and [`reduceByKey`](programming-guide.html#transformations) transformations to compute the per-word counts in the file as an RDD of (string, int) pairs. To collect the word counts in our shell, we can use the [`collect`](programming-guide.html#actions) action:

{% highlight python %}
>>> wordCounts.collect()
[(u'and', 9), (u'A', 1), (u'webpage', 1), (u'README', 1), (u'Note', 1), (u'"local"', 1), (u'variable', 1), ...]
{% endhighlight %}

</div>
</div>

## Caching
Spark also supports pulling data sets into a cluster-wide in-memory cache. This is very useful when data is accessed repeatedly, such as when querying a small "hot" dataset or when running an iterative algorithm like PageRank. As a simple example, let's mark our `linesWithSpark` dataset to be cached:

<div class="codetabs">
<div data-lang="scala" markdown="1">

{% highlight scala %}
scala> linesWithSpark.cache()
res7: linesWithSpark.type = MapPartitionsRDD[2] at filter at <console>:27

scala> linesWithSpark.count()
res8: Long = 19

scala> linesWithSpark.count()
res9: Long = 19
{% endhighlight %}

It may seem silly to use Spark to explore and cache a 100-line text file. The interesting part is
that these same functions can be used on very large data sets, even when they are striped across
tens or hundreds of nodes. You can also do this interactively by connecting `bin/spark-shell` to
a cluster, as described in the [programming guide](programming-guide.html#initializing-spark).

</div>
<div data-lang="python" markdown="1">

{% highlight python %}
>>> linesWithSpark.cache()

>>> linesWithSpark.count()
19

>>> linesWithSpark.count()
19
{% endhighlight %}

It may seem silly to use Spark to explore and cache a 100-line text file. The interesting part is
that these same functions can be used on very large data sets, even when they are striped across
tens or hundreds of nodes. You can also do this interactively by connecting `bin/pyspark` to
a cluster, as described in the [programming guide](programming-guide.html#initializing-spark).

</div>
</div>

# Self-Contained Applications
Suppose we wish to write a self-contained application using the Spark API. We will walk through a
simple application in Scala (with sbt), Java (with Maven), and Python.

<div class="codetabs">
<div data-lang="scala" markdown="1">

We'll create a very simple Spark application in Scala--so simple, in fact, that it's
named `SimpleApp.scala`:

{% highlight scala %}
/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
{% endhighlight %}

Note that applications should define a `main()` method instead of extending `scala.App`.
Subclasses of `scala.App` may not work correctly.

This program just counts the number of lines containing 'a' and the number containing 'b' in the
Spark README. Note that you'll need to replace YOUR_SPARK_HOME with the location where Spark is
installed. Unlike the earlier examples with the Spark shell, which initializes its own SparkContext,
we initialize a SparkContext as part of the program.

We pass the SparkContext constructor a 
[SparkConf](api/scala/index.html#org.apache.spark.SparkConf)
object which contains information about our
application. 

Our application depends on the Spark API, so we'll also include an sbt configuration file, 
`simple.sbt`, which explains that Spark is a dependency. This file also adds a repository that 
Spark depends on:

{% highlight scala %}
name := "Simple Project"

version := "1.0"

scalaVersion := "{{site.SCALA_VERSION}}"

libraryDependencies += "org.apache.spark" %% "spark-core" % "{{site.SPARK_VERSION}}"
{% endhighlight %}

For sbt to work correctly, we'll need to layout `SimpleApp.scala` and `simple.sbt`
according to the typical directory structure. Once that is in place, we can create a JAR package
containing the application's code, then use the `spark-submit` script to run our program.

{% highlight bash %}
# Your directory layout should look like this
$ find .
.
./simple.sbt
./src
./src/main
./src/main/scala
./src/main/scala/SimpleApp.scala

# Package a jar containing your application
$ sbt package
...
[info] Packaging {..}/{..}/target/scala-2.10/simple-project_2.10-1.0.jar

# Use spark-submit to run your application
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class "SimpleApp" \
  --master local[4] \
  target/scala-2.10/simple-project_2.10-1.0.jar
...
Lines with a: 46, Lines with b: 23
{% endhighlight %}

</div>
<div data-lang="java" markdown="1">
This example will use Maven to compile an application JAR, but any similar build system will work.

We'll create a very simple Spark application, `SimpleApp.java`:

{% highlight java %}
/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
  public static void main(String[] args) {
    String logFile = "YOUR_SPARK_HOME/README.md"; // Should be some file on your system
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("a"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("b"); }
    }).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
  }
}
{% endhighlight %}

This program just counts the number of lines containing 'a' and the number containing 'b' in a text
file. Note that you'll need to replace YOUR_SPARK_HOME with the location where Spark is installed.
As with the Scala example, we initialize a SparkContext, though we use the special
`JavaSparkContext` class to get a Java-friendly one. We also create RDDs (represented by
`JavaRDD`) and run transformations on them. Finally, we pass functions to Spark by creating classes
that extend `spark.api.java.function.Function`. The
[Spark programming guide](programming-guide.html) describes these differences in more detail.

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
      <artifactId>spark-core_{{site.SCALA_BINARY_VERSION}}</artifactId>
      <version>{{site.SPARK_VERSION}}</version>
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

As an example, we'll create a simple Spark application, `SimpleApp.py`:

{% highlight python %}
"""SimpleApp.py"""
from pyspark import SparkContext

logFile = "YOUR_SPARK_HOME/README.md"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
{% endhighlight %}


This program just counts the number of lines containing 'a' and the number containing 'b' in a
text file.
Note that you'll need to replace YOUR_SPARK_HOME with the location where Spark is installed.
As with the Scala and Java examples, we use a SparkContext to create RDDs.
We can pass Python functions to Spark, which are automatically serialized along with any variables
that they reference.
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

</div>
</div>

# Where to Go from Here
Congratulations on running your first Spark application!

* For an in-depth overview of the API, start with the [Spark programming guide](programming-guide.html),
  or see "Programming Guides" menu for other components.
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
