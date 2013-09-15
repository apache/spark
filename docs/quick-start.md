---
layout: global
title: Quick Start
---

* This will become a table of contents (this text will be scraped).
{:toc}

This tutorial provides a quick introduction to using Spark. We will first introduce the API through Spark's interactive Scala shell (don't worry if you don't know Scala -- you will not need much for this), then show how to write standalone applications in Scala, Java, and Python.
See the [programming guide](scala-programming-guide.html) for a more complete reference.

To follow along with this guide, you only need to have successfully built Spark on one machine. Simply go into your Spark directory and run:

{% highlight bash %}
$ sbt/sbt assembly
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
res1: String = # Apache Spark
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

## More on RDD Operations
RDD actions and transformations can be used for more complex computations. Let's say we want to find the line with the most words:

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

# A Standalone App in Scala
Now say we wanted to write a standalone application using the Spark API. We will walk through a simple application in both Scala (with SBT), Java (with Maven), and Python. If you are using other build systems, consider using the Spark assembly JAR described in the developer guide.

We'll create a very simple Spark application in Scala. So simple, in fact, that it's named `SimpleApp.scala`:

{% highlight scala %}
/*** SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "$YOUR_SPARK_HOME/README.md" // Should be some file on your system
    val sc = new SparkContext("local", "Simple App", "YOUR_SPARK_HOME",
      List("target/scala-{{site.SCALA_VERSION}}/simple-project_{{site.SCALA_VERSION}}-1.0.jar"))
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
{% endhighlight %}

This program simply counts the number of lines containing 'a' and the number containing 'b' in the Spark README. Note that you'll need to replace $YOUR_SPARK_HOME with the location where Spark is installed. Unlike the earlier examples with the Spark shell, which initializes its own SparkContext, we initialize a SparkContext as part of the proogram. We pass the SparkContext constructor four arguments, the type of scheduler we want to use (in this case, a local scheduler), a name for the application, the directory where Spark is installed, and a name for the jar file containing the application's code. The final two arguments are needed in a distributed setting, where Spark is running across several nodes, so we include them for completeness. Spark will automatically ship the jar files you list to slave nodes.

This file depends on the Spark API, so we'll also include an sbt configuration file, `simple.sbt` which explains that Spark is a dependency. This file also adds a repository that Spark depends on:

{% highlight scala %}
name := "Simple Project"

version := "1.0"

scalaVersion := "{{site.SCALA_VERSION}}"

libraryDependencies += "org.apache.spark" %% "spark-core" % "{{site.SPARK_VERSION}}"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
{% endhighlight %}

If you also wish to read data from Hadoop's HDFS, you will also need to add a dependency on `hadoop-client` for your version of HDFS:

{% highlight scala %}
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "<your-hdfs-version>"
{% endhighlight %}

Finally, for sbt to work correctly, we'll need to layout `SimpleApp.scala` and `simple.sbt` according to the typical directory structure. Once that is in place, we can create a JAR package containing the application's code, then use `sbt run` to execute our program.

{% highlight bash %}
$ find .
.
./simple.sbt
./src
./src/main
./src/main/scala
./src/main/scala/SimpleApp.scala

$ sbt package
$ sbt run
...
Lines with a: 46, Lines with b: 23
{% endhighlight %}

# A Standalone App in Java
Now say we wanted to write a standalone application using the Java API. We will walk through doing this with Maven. If you are using other build systems, consider using the Spark assembly JAR described in the developer guide.

We'll create a very simple Spark application, `SimpleApp.java`:

{% highlight java %}
/*** SimpleApp.java ***/
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
  public static void main(String[] args) {
    String logFile = "$YOUR_SPARK_HOME/README.md"; // Should be some file on your system
    JavaSparkContext sc = new JavaSparkContext("local", "Simple App",
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

This program simply counts the number of lines containing 'a' and the number containing 'b' in a system log file. Note that you'll need to replace $YOUR_SPARK_HOME with the location where Spark is installed. As with the Scala example, we initialize a SparkContext, though we use the special `JavaSparkContext` class to get a Java-friendly one. We also create RDDs (represented by `JavaRDD`) and run transformations on them. Finally, we pass functions to Spark by creating classes that extend `spark.api.java.function.Function`. The [Java programming guide](java-programming-guide.html) describes these differences in more detail.

To build the program, we also write a Maven `pom.xml` file that lists Spark as a dependency. Note that Spark artifacts are tagged with a Scala version.

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
      <id>Akka repository</id>
      <url>http://repo.akka.io/releases</url>
    </repository>
  </repositories>
  <dependencies>
    <dependency> <!-- Spark dependency -->
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-core_{{site.SCALA_VERSION}}</artifactId>
      <version>{{site.SPARK_VERSION}}</version>
    </dependency>
  </dependencies>
</project>
{% endhighlight %}

If you also wish to read data from Hadoop's HDFS, you will also need to add a dependency on `hadoop-client` for your version of HDFS:

{% highlight xml %}
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-client</artifactId>
      <version>...</version>
    </dependency>
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

Now, we can execute the application using Maven:

{% highlight bash %}
$ mvn package
$ mvn exec:java -Dexec.mainClass="SimpleApp"
...
Lines with a: 46, Lines with b: 23
{% endhighlight %}

# A Standalone App in Python
Now we will show how to write a standalone application using the Python API (PySpark).

As an example, we'll create a simple Spark application, `SimpleApp.py`:

{% highlight python %}
"""SimpleApp.py"""
from pyspark import SparkContext

logFile = "$YOUR_SPARK_HOME/README.md"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print "Lines with a: %i, lines with b: %i" % (numAs, numBs)
{% endhighlight %}


This program simply counts the number of lines containing 'a' and the number containing 'b' in a system log file.
Note that you'll need to replace $YOUR_SPARK_HOME with the location where Spark is installed. 
As with the Scala and Java examples, we use a SparkContext to create RDDs.
We can pass Python functions to Spark, which are automatically serialized along with any variables that they reference.
For applications that use custom classes or third-party libraries, we can add those code dependencies to SparkContext to ensure that they will be available on remote machines; this is described in more detail in the [Python programming guide](python-programming-guide.html).
`SimpleApp` is simple enough that we do not need to specify any code dependencies.

We can run this application using the `pyspark` script:

{% highlight python %}
$ cd $SPARK_HOME
$ ./pyspark SimpleApp.py
...
Lines with a: 46, Lines with b: 23
{% endhighlight python %}

# Running on a Cluster

There are a few additional considerations when running applicaitons on a 
[Spark](spark-standalone.html), [YARN](running-on-yarn.html), or 
[Mesos](running-on-mesos.html) cluster.

### Including Your Dependencies
If your code depends on other projects, you will need to ensure they are also
present on the slave nodes. A popular approach is to create an
assembly jar (or "uber" jar) containing your code and its dependencies. Both
[sbt](https://github.com/sbt/sbt-assembly) and 
[Maven](http://maven.apache.org/plugins/maven-assembly-plugin/) 
have assembly plugins. When creating assembly jars, list Spark 
itself as a `provided` dependency; it need not be bundled since it is 
already present on the slaves. Once you have an assembled jar, 
add it to the SparkContext as shown here. It is also possible to submit 
your dependent jars one-by-one when creating a SparkContext.

### Setting Configuration Options
Spark includes several configuration options which influence the behavior
of your application. These should be set as 
[JVM system properties](configuration.html#system-properties) in your 
program. The options will be captured and shipped to all slave nodes.

### Accessing Hadoop Filesystems

The examples here access a local file. To read data from a distributed
filesystem, such as HDFS, include 
[Hadoop version information](index.html#a-note-about-hadoop-versions)
in your build file. By default, Spark builds against HDFS 1.0.4.
