---
layout: global
title: Spark Quick Start
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Introduction

This document provides a quick-and-dirty look at Spark's API. See the [programming guide]({{HOME_PATH}}/scala-programming-guide.html) for a complete reference. To follow along with this guide, you only need to have successfully [built spark]({{HOME_PATH}}) on one machine -- all operations are demonstrated locally.

# Interactive Data Analysis with the Spark Shell

## Shell basics

Start the Spark shell by executing `./spark-shell` in the Spark directory.

Spark's primary abstraction is a distributed collection of items called a Resilient Distributed Dataset (RDD). RDD's can be created from Hadoop InputFormat's (such as HDFS files) or by transforming other RDD's. Let's make a new RDD derived from the text of the README file in the Spark source directory:

{% highlight scala %}
scala> val textFile = sc.textFile("README.md")
textFile: spark.RDD[String] = spark.MappedRDD@2ee9b6e3
{% endhighlight %}

RDD's have _actions_, which return values, and _transformations_, which return pointers to new RDD's. Let's start with a few actions:

{% highlight scala %}
scala> textFile.count() // Number of items in this RDD
res0: Long = 74

scala> textFile.first() // First item in this RDD
res1: String = # Spark
{% endhighlight %}

Now let's use a transformation. We will use the `filter()` function to return a new RDD with a subset of the items in the file.

{% highlight scala %}
scala> val sparkLinesOnly = textFile.filter(line => line.contains("Spark"))
sparkLinesOnly: spark.RDD[String] = spark.FilteredRDD@7dd4af09
{% endhighlight %}

We can chain together transformations and actions:

{% highlight scala %}
scala> textFile.filter(line => line.contains("Spark")).count() // How many lines contain "Spark"?
res3: Long = 15
{% endhighlight %}

## Data flow
RDD transformations can be used for more complex computations. Let's say we want to find the line with the most words:

{% highlight scala %}
scala> textFile.map(line => line.split(" ").size).reduce((a, b) => if (a < b) {b} else {a})
res4: Long = 16
{% endhighlight %}

This first maps a line to an integer value, creating a new RDD. `reduce` is called on that RDD to find the largest line count. The arguments to map() and reduce() are scala closures. We can easily include functions declared elsewhere, or include existing functions in our anonymous closures. For instance, we can use `Math.max()` to make this code easier to understand. 

{% highlight scala %}
scala> import java.lang.Math;
import java.lang.Math

scala> textFile.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b))
res5: Int = 16
{% endhighlight %}

## Caching
Spark also supports pulling data sets into a cluster-wide cache. This is very useful when data is accessed iteratively, such as in machine learning jobs, or repeatedly, such as when small "hot data" is queried repeatedly. As a simple example, let's pull part of our file into memory:


{% highlight scala %}
scala> val linesWithSparkCached = linesWithSpark.cache()
linesWithSparkCached: spark.RDD[String] = spark.FilteredRDD@17e51082

scala> linesWithSparkCached.count()
res6: Long = 15

scala> linesWithSparkCached.count()
res7: Long = 15
{% endhighlight %}

It may seem silly to use a Spark to explore and cache a 30 line text file. The interesting part is that these same functions can be used on very large data sets, even when they are striped across tens or hundreds of nodes.

# A Spark Job
Now say we wanted to write custom job using the Spark API. We will walk through a simple job in both Scala (with sbt) and Java (with maven). If you using other build systems, please reference the Spark assembly jar in the developer guide. The first step is to publish spark to our local Ivy/Maven repositories. From the spark directory

{% highlight bash %}
$ sbt/sbt publish-local
{% endhighlight %}

## In Scala
Next, we'll create a very simple Spark job in Scala. So simple, in fact, that it's named `SimpleJob.scala`:

{% highlight scala %}
/*** SimpleJob.scala ***/
import spark.SparkContext
import SparkContext._

object SimpleJob extends Application {
  val logFile = "/var/log/syslog" // Should be some log file on your system
  val sc = new SparkContext("local", "Simple Job")
  val logData = sc.textFile(logFile, 2).cache()
  val numAs = logData.filter(line => line.contains("a")).count()
  val numBs = logData.filter(line => line.contains("b")).count()
  println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
}
{% endhighlight %}

This file depends on the Spark API, so we'll also include an sbt configuration file, `simple.sbt` which explains that Spark is a dependency:

{% highlight scala %}
name := "Simple Project"

version := "1.0"

libraryDependencies += "org.spark-project" %% "spark-core" % "0.6.0-SNAPSHOT"
{% endhighlight %}

Of course, for sbt to work correctly, we'll need to layout `SimpleJob.scala` and `simple.sbt` according to the typical directory structure. Once that is in place, we can use `sbt run` to execute our example job.

{% highlight bash %}
$ find . 
.
./simple.sbt
./src
./src/main
./src/main/scala
./src/main/scala/SimpleJob.scala

$ sbt clean run
...
Lines with a: 8422, Lines with b: 1836
{% endhighlight %}

## In Java
Our simple job in Java (`SimpleJob.java`) looks very similar:

{% highlight java %}
/*** SimpleJob.java ***/
import spark.api.java.*;
import spark.api.java.function.Function;

public class SimpleJob {
  public static void main(String[] args) {
    String logFile = "/var/log/syslog"; // Should be some log file on your system
    JavaSparkContext sc = new JavaSparkContext("local", "Simple Job");
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("a"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("b"); }
    }).count();

    System.out.println(String.format(
        "Lines with a: %s, Lines with b: %s", numAs, numBs));
  }
}
{% endhighlight %}

Our Maven `pom.xml` file will list Spark as a dependency. Note that Spark artifacts are tagged with a Scala version.

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
      <groupId>org.spark-project</groupId>
      <artifactId>spark-core_2.9.2</artifactId>
      <version>0.6.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
</project>
{% endhighlight %}

To make Maven happy, we lay out these files according to the canonical directory structure:
{% highlight bash %}
$ find .
./pom.xml
./src
./src/main
./src/main/java
./src/main/java/SimpleJob.java
{% endhighlight %}

Now, we can execute the job using Maven. Of course, in practice, we would typically compile or package this job and run it outside of Maven.
{% highlight bash %}
$ mvn clean exec:java -Dexec.mainClass="SimpleJob"
...
Lines with a: 8422, Lines with b: 1836
{% endhighlight %}
