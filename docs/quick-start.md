---
layout: global
title: Quick Start
---

* This will become a table of contents (this text will be scraped).
{:toc}

This tutorial provides a quick introduction to using Spark. We will first introduce the API through Spark's interactive Scala shell (don't worry if you don't know Scala -- you will not need much for this), then show how to write standalone jobs in Scala, Java, and Python.
See the [programming guide](scala-programming-guide.html) for a more complete reference.

To follow along with this guide, you only need to have successfully built Spark on one machine. Simply go into your Spark directory and run:

{% highlight bash %}
$ sbt/sbt package
{% endhighlight %}

# Interactive Analysis with the Spark Shell

## Basics

Spark's interactive shell provides a simple way to learn the API, as well as a powerful tool to analyze datasets interactively.
Start the shell by running `./spark-shell` in the Spark directory.

Spark's primary abstraction is a distributed collection of items called a Resilient Distributed Dataset (RDD). RDDs can be created from Hadoop InputFormats (such as HDFS files) or by transforming other RDDs. Let's make a new RDD from the text of the README file in the Spark source directory:

{% highlight scala %}
scala> val textFile = sc.textFile("README.md")
textFile: spark.RDD[String] = spark.MappedRDD@2ee9b6e3
{% endhighlight %}

RDDs have _[actions](scala-programming-guide.html#actions)_, which return values, and _[transformations](scala-programming-guide.html#transformations)_, which return pointers to new RDDs. Let's start with a few actions:

{% highlight scala %}
scala> textFile.count() // Number of items in this RDD
res0: Long = 74

scala> textFile.first() // First item in this RDD
res1: String = # Spark
{% endhighlight %}

Now let's use a transformation. We will use the [`filter`](scala-programming-guide.html#transformations) transformation to return a new RDD with a subset of the items in the file.

{% highlight scala %}
scala> val linesWithSpark = textFile.filter(line => line.contains("Spark"))
linesWithSpark: spark.RDD[String] = spark.FilteredRDD@7dd4af09
{% endhighlight %}

We can chain together transformations and actions:

{% highlight scala %}
scala> textFile.filter(line => line.contains("Spark")).count() // How many lines contain "Spark"?
res3: Long = 15
{% endhighlight %}

## Transformations
RDD transformations can be used for more complex computations. Let's say we want to find the line with the most words:

{% highlight scala %}
scala> textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
res4: Long = 16
{% endhighlight %}

This first maps a line to an integer value, creating a new RDD. `reduce` is called on that RDD to find the largest line count. The arguments to `map` and `reduce` are Scala function literals (closures), and can use any language feature or Scala/Java library. For example, we can easily call functions declared elsewhere. We'll use `Math.max()` function to make this code easier to understand:

{% highlight scala %}
scala> import java.lang.Math
import java.lang.Math

scala> textFile.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b))
res5: Int = 16
{% endhighlight %}

One common data flow pattern is MapReduce, as popularized by Hadoop. Spark can implement MapReduce flows easily:

{% highlight scala %}
scala> val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
wordCounts: spark.RDD[(java.lang.String, Int)] = spark.ShuffledAggregatedRDD@71f027b8
{% endhighlight %}

Here, we combined the [`flatMap`](scala-programming-guide.html#transformations), [`map`](scala-programming-guide.html#transformations) and [`reduceByKey`](scala-programming-guide.html#transformations) transformations to compute the per-word counts in the file as an RDD of (String, Int) pairs. To collect the word counts in our shell, we can use the [`collect`](scala-programming-guide.html#actions) action:

{% highlight scala %}
scala> wordCounts.collect()
res6: Array[(java.lang.String, Int)] = Array((need,2), ("",43), (Extra,3), (using,1), (passed,1), (etc.,1), (its,1), (`/usr/local/lib/libmesos.so`,1), (`SCALA_HOME`,1), (option,1), (these,1), (#,1), (`PATH`,,2), (200,1), (To,3),...
{% endhighlight %}

## Caching
Spark also supports pulling data sets into a cluster-wide in-memory cache. This is very useful when data is accessed repeatedly, such as when querying a small "hot" dataset or when running an iterative algorithm like PageRank. As a simple example, let's mark our `linesWithSpark` dataset to be cached:

{% highlight scala %}
scala> linesWithSpark.cache()
res7: spark.RDD[String] = spark.FilteredRDD@17e51082

scala> linesWithSpark.count()
res8: Long = 15

scala> linesWithSpark.count()
res9: Long = 15
{% endhighlight %}

It may seem silly to use Spark to explore and cache a 30-line text file. The interesting part is that these same functions can be used on very large data sets, even when they are striped across tens or hundreds of nodes. You can also do this interactively by connecting `spark-shell` to a cluster, as described in the [programming guide](scala-programming-guide.html#initializing-spark).

# A Standalone Job in Scala
Now say we wanted to write a standalone job using the Spark API. We will walk through a simple job in both Scala (with sbt) and Java (with maven). If you are using other build systems, consider using the Spark assembly JAR described in the developer guide.

We'll create a very simple Spark job in Scala. So simple, in fact, that it's named `SimpleJob.scala`:

{% highlight scala %}
/*** SimpleJob.scala ***/
import spark.SparkContext
import SparkContext._

object SimpleJob {
  def main(args: Array[String]) {
    val logFile = "/var/log/syslog" // Should be some file on your system
    val sc = new SparkContext("local", "Simple Job", "$YOUR_SPARK_HOME",
      List("target/scala-{{site.SCALA_VERSION}}/simple-project_{{site.SCALA_VERSION}}-1.0.jar"))
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
{% endhighlight %}

This job simply counts the number of lines containing 'a' and the number containing 'b' in a system log file. Unlike the earlier examples with the Spark shell, which initializes its own SparkContext, we initialize a SparkContext as part of the job. We pass the SparkContext constructor four arguments, the type of scheduler we want to use (in this case, a local scheduler), a name for the job, the directory where Spark is installed, and a name for the jar file containing the job's sources. The final two arguments are needed in a distributed setting, where Spark is running across several nodes, so we include them for completeness. Spark will automatically ship the jar files you list to slave nodes.

This file depends on the Spark API, so we'll also include an sbt configuration file, `simple.sbt` which explains that Spark is a dependency. This file also adds two repositories which host Spark dependencies:

{% highlight scala %}
name := "Simple Project"

version := "1.0"

scalaVersion := "{{site.SCALA_VERSION}}"

libraryDependencies += "org.spark-project" %% "spark-core" % "{{site.SPARK_VERSION}}"

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.cc/")
{% endhighlight %}

Of course, for sbt to work correctly, we'll need to layout `SimpleJob.scala` and `simple.sbt` according to the typical directory structure. Once that is in place, we can create a JAR package containing the job's code, then use `sbt run` to execute our example job.

{% highlight bash %}
$ find .
.
./simple.sbt
./src
./src/main
./src/main/scala
./src/main/scala/SimpleJob.scala

$ sbt package
$ sbt run
...
Lines with a: 8422, Lines with b: 1836
{% endhighlight %}

This example only runs the job locally; for a tutorial on running jobs across several machines, see the [Standalone Mode](spark-standalone.html) documentation, and consider using a distributed input source, such as HDFS.

# A Standalone Job In Java
Now say we wanted to write a standalone job using the Java API. We will walk through doing this with Maven. If you are using other build systems, consider using the Spark assembly JAR described in the developer guide.

We'll create a very simple Spark job, `SimpleJob.java`:

{% highlight java %}
/*** SimpleJob.java ***/
import spark.api.java.*;
import spark.api.java.function.Function;

public class SimpleJob {
  public static void main(String[] args) {
    String logFile = "/var/log/syslog"; // Should be some file on your system
    JavaSparkContext sc = new JavaSparkContext("local", "Simple Job",
      "$YOUR_SPARK_HOME", new String[]{"target/simple-project-1.0.jar"});
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

This job simply counts the number of lines containing 'a' and the number containing 'b' in a system log file. Note that like in the Scala example, we initialize a SparkContext, though we use the special `JavaSparkContext` class to get a Java-friendly one. We also create RDDs (represented by `JavaRDD`) and run transformations on them. Finally, we pass functions to Spark by creating classes that extend `spark.api.java.function.Function`. The [Java programming guide](java-programming-guide.html) describes these differences in more detail.

To build the job, we also write a Maven `pom.xml` file that lists Spark as a dependency. Note that Spark artifacts are tagged with a Scala version.

{% highlight xml %}
<project>
  <groupId>edu.berkeley</groupId>
  <artifactId>simple-project</artifactId>
  <modelVersion>4.0.0</modelVersion>
  <name>Simple Project</name>
  <packaging>jar</packaging>
  <version>1.0</version>
  <repositories>
    <repository>
      <id>Spray.cc repository</id>
      <url>http://repo.spray.cc</url>
    </repository>
    <repository>
      <id>Akka repository</id>
      <url>http://repo.akka.io/releases</url>
    </repository>
  </repositories>
  <dependencies>
    <dependency> <!-- Spark dependency -->
      <groupId>org.spark-project</groupId>
      <artifactId>spark-core_{{site.SCALA_VERSION}}</artifactId>
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
./src/main/java/SimpleJob.java
{% endhighlight %}

Now, we can execute the job using Maven:

{% highlight bash %}
$ mvn package
$ mvn exec:java -Dexec.mainClass="SimpleJob"
...
Lines with a: 8422, Lines with b: 1836
{% endhighlight %}

This example only runs the job locally; for a tutorial on running jobs across several machines, see the [Standalone Mode](spark-standalone.html) documentation, and consider using a distributed input source, such as HDFS.

# A Standalone Job In Python
Now we will show how to write a standalone job using the Python API (PySpark).

As an example, we'll create a simple Spark job, `SimpleJob.py`:

{% highlight python %}
"""SimpleJob.py"""
from pyspark import SparkContext

logFile = "/var/log/syslog"  # Should be some file on your system
sc = SparkContext("local", "Simple job")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print "Lines with a: %i, lines with b: %i" % (numAs, numBs)
{% endhighlight %}


This job simply counts the number of lines containing 'a' and the number containing 'b' in a system log file.
Like in the Scala and Java examples, we use a SparkContext to create RDDs.
We can pass Python functions to Spark, which are automatically serialized along with any variables that they reference.
For jobs that use custom classes or third-party libraries, we can add those code dependencies to SparkContext to ensure that they will be available on remote machines; this is described in more detail in the [Python programming guide](python-programming-guide.html).
`SimpleJob` is simple enough that we do not need to specify any code dependencies.

We can run this job using the `pyspark` script:

{% highlight python %}
$ cd $SPARK_HOME
$ ./pyspark SimpleJob.py
...
Lines with a: 8422, Lines with b: 1836
{% endhighlight python %}

This example only runs the job locally; for a tutorial on running jobs across several machines, see the [Standalone Mode](spark-standalone.html) documentation, and consider using a distributed input source, such as HDFS.
