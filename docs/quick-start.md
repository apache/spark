---
layout: global
title: Spark Programming Quick Start Guide
---

* This will become a table of contents (this text will be scraped).
{:toc}

This tutorial provides a quick introduction to using Spark. We'll introduce the API through Spark's interactive Scala shell (don't worry if you don't know Scala -- you won't need much for this), then we'll show you how to write standalone applications in Scala, Java, and Python.
See the [Spark Scala Programming Guide](scala-programming-guide.html) for a more complete reference.

To follow along with this guide, you only need to have successfully built Spark on one machine. If you haven't built Spark yet, see the [Building](index.html#building) section of Getting Started with Apache Spark.

# Interactive Analysis with the Spark Shell

Spark's interactive shell provides a simple way to learn the Spark API, and it's also a powerful tool for interactively analyzing datasets.

To start the shell, from the top-level Spark directory:

    $ ./bin/spark-shell
    ...
    scala>

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

Now let's use a transformation. We'll use the [`filter`](scala-programming-guide.html#transformations) transformation to return a new RDD with a subset of the items in the file:

{% highlight scala %}
scala> val linesWithSpark = textFile.filter(line => line.contains("Spark"))
linesWithSpark: spark.RDD[String] = spark.FilteredRDD@7dd4af09
{% endhighlight %}

Now we'll chain together transformations and actions:

{% highlight scala %}
scala> textFile.filter(line => line.contains("Spark")).count() // How many lines contain "Spark"?
res3: Long = 15
{% endhighlight %}

## More on RDD Operations
RDD actions and transformations can be used for more complex computations. Let's find the line with the most words:

{% highlight scala %}
scala> textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
res4: Long = 16
{% endhighlight %}

This maps a line to an integer value, creating a new RDD, then `reduce` is called on that RDD to find the largest line count. The arguments to `map` and `reduce` are Scala function literals (closures), and can use any language feature or Scala/Java library. For example, we can easily call functions declared elsewhere. We'll use the `Math.max()` function to make this code easier to understand:

{% highlight scala %}
scala> import java.lang.Math
import java.lang.Math

scala> textFile.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b))
res5: Int = 16
{% endhighlight %}

One common data flow pattern is MapReduce, as popularized by Hadoop. Spark can easily implement MapReduce flows:

{% highlight scala %}
scala> val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
wordCounts: spark.RDD[(java.lang.String, Int)] = spark.ShuffledAggregatedRDD@71f027b8
{% endhighlight %}

We combined the [`flatMap`](scala-programming-guide.html#transformations), [`map`](scala-programming-guide.html#transformations) and [`reduceByKey`](scala-programming-guide.html#transformations) transformations to compute the per-word counts in the file as an RDD of (String, Int) pairs. To collect the word counts in our shell, we'll use the [`collect`](scala-programming-guide.html#actions) action:

{% highlight scala %}
scala> wordCounts.collect()
res6: Array[(java.lang.String, Int)] = Array((need,2), ("",43), (Extra,3), (using,1), (passed,1), (etc.,1), (its,1), (`/usr/local/lib/libmesos.so`,1), (`SCALA_HOME`,1), (option,1), (these,1), (#,1), (`PATH`,,2), (200,1), (To,3),...
{% endhighlight %}

## Caching
Spark also supports pulling data sets into a cluster-wide in-memory cache. This is very useful when data is accessed repeatedly, such as when querying a small "hot" dataset or when running an iterative algorithm like PageRank. 

As a simple example, let's cache our `linesWithSpark` dataset:

{% highlight scala %}
scala> linesWithSpark.cache()
res7: spark.RDD[String] = spark.FilteredRDD@17e51082

scala> linesWithSpark.count()
res8: Long = 15
{% endhighlight %}

It may seem silly to use Spark to explore and cache a 30-line text file. What's interesting is that these same functions can be used on very large data sets, even when they are striped across tens or hundreds of nodes. You can also do this interactively by connecting `bin/spark-shell` to a cluster, as described in the [Spark Scala Programming Guide](scala-programming-guide.html#initializing-spark).

# A Standalone App in Scala
Now we'll write a standalone application using the Spark Scala API. We'll walk through a simple application in Scala (with SBT), Java (with Maven), and Python. If you're using other build systems, consider using the Spark assembly JAR described in the [Linking with Spark](scala-programming-guide.html#linking-with-spark) section of the Spark Scala Programming Guide.

We'll create a very simple Spark application in Scala, so simple it's named `SimpleApp.scala`:

{% highlight scala %}
/*** SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "$YOUR_SPARK_HOME/README.md" // Should be some file on your system
    val sc = new SparkContext("local", "Simple App", "YOUR_SPARK_HOME",
      List("target/scala-{{site.SCALA_BINARY_VERSION}}/simple-project_{{site.SCALA_BINARY_VERSION}}-1.0.jar"))
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
{% endhighlight %}

This program counts the number of lines containing 'a' and the number of lines containing 'b' in the Spark README. Note that you'll need to replace $YOUR_SPARK_HOME with an environment variable pointing to your Spark installation directory. Unlike the earlier Spark shell examples, which initialize their own SparkContext, we initialize a SparkContext as part of the program. 

We pass four arguments to the SparkContext constructor: the type of scheduler we want to use (in this case, a local scheduler), a name for the application, the directory where Spark is installed, and a name for the JAR file containing the application's code. 

The final two arguments are needed in a distributed setting, where Spark is running across several nodes, so we're including them for completeness. Spark will automatically ship the listed JAR files to your slave nodes.

This file depends on the Spark API, so we'll also include the SBT configuration file `simple.sbt` that specifies that Spark is a dependency. This file also adds a repository that Spark depends on:

{% highlight scala %}
name := "Simple Project"
version := "1.0"
scalaVersion := "{{site.SCALA_VERSION}}"
libraryDependencies += "org.apache.spark" %% "spark-core" % "{{site.SPARK_VERSION}}"
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
{% endhighlight %}

If you also want to read data from HDFS, you'll need to add a dependency on `hadoop-client` for your version of HDFS:

{% highlight scala %}
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "<your-hdfs-version>"
{% endhighlight %}

Finally, for SBT to work correctly, we need to layout `SimpleApp.scala` and `simple.sbt` according to the typical directory structure. Once that is in place, we can create a JAR package containing the application's code, then use `sbt/sbt run` to execute our program:

{% highlight bash %}
$ find .
.
./simple.sbt
./src
./src/main
./src/main/scala
./src/main/scala/SimpleApp.scala

$ sbt/sbt package
$ sbt/sbt run
...
Lines with a: 46, Lines with b: 23
{% endhighlight %}

# A Standalone App in Java
Now we'll write a standalone application using the Spark Java API. We'll do this using Maven. If you're using other build systems, consider using the Spark assembly JAR described in the [Linking with Spark](scala-programming-guide.html#linking-with-spark) section of the Spark Scala Programming Guide.

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

This program counts the number of lines containing 'a' and the number of lines containing 'b' in a text file. Note that you'll need to replace $YOUR_SPARK_HOME with an environment variable that points to your Spark installation directory. As with the Scala example, we initialize a SparkContext, though we use the special `JavaSparkContext` class to get a Java-friendly one. 

We also create RDDs (represented by `JavaRDD`) and run transformations on them. Finally, we pass functions to Spark by creating classes that extend `spark.api.java.function.Function`. The [Spark Java Programming Guide](java-programming-guide.html) describes these differences in more detail.

To build the program, we also write a Maven `pom.xml` file that lists Spark as a dependency. Note that Spark artifacts are tagged with a Scala version:

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
      <artifactId>spark-core_{{site.SCALA_BINARY_VERSION}}</artifactId>
      <version>{{site.SPARK_VERSION}}</version>
    </dependency>
  </dependencies>
</project>
{% endhighlight %}

If you want to read data from HDFS, you'll also need to add a dependency on `hadoop-client` for your version of HDFS:

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

Now we'll execute the application using Maven:

{% highlight bash %}
$ mvn package
$ mvn exec:java -Dexec.mainClass="SimpleApp"
...
Lines with a: 46, Lines with b: 23
{% endhighlight %}

# A Standalone App in Python
Now we'll write a standalone application using the Spark Python API (PySpark). We'll create a simple Spark application, `SimpleApp.py`:

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

This program counts the number of lines containing 'a' and the number of lines containing 'b' in a text file.
Note that you'll need to replace $YOUR_SPARK_HOME with an environment variable pointing to your Spark installation directory.

As with the Scala and Java examples, we use a SparkContext to create RDDs.
We can pass Python functions to Spark, which are automatically serialized along with any variables that they reference.

`SimpleApp` is simple enough that we do not need to specify any code dependencies. However, we can add code dependencies to SparkContext for applications that do use custom classes or third-party libraries to ensure that the dependencies will be available on remote machines. This is described in more detail in the [Standalone Programs](python-programming-guide.html#standalone-programs) section of the Spark Python Programming Guide.

We'll run this application using the `bin/pyspark` script from the top-level Spark directory:

{% highlight python %}
$ ./bin/pyspark SimpleApp.py
...
Lines with a: 46, Lines with b: 23
{% endhighlight python %}

# Running on a Cluster

There are a few additional considerations when running applications on a 
[Spark](spark-standalone.html), [YARN](running-on-yarn.html), or 
[Mesos](running-on-mesos.html) cluster.

### Including Your Dependencies
If your code depends on other projects, you'll need to ensure they are also
present on the slave nodes. A popular approach is to create an
assembly JAR (or "uber" JAR) containing your code and its dependencies. [SBT](https://github.com/sbt/sbt-assembly) and 
[Maven](http://maven.apache.org/plugins/maven-assembly-plugin/) 
both have assembly plugins. 

When creating assembly JARs, list Spark 
itself as a `provided` dependency. It doesn't need to be bundled because it's 
already present on the slaves. 

For Python, you can add the assembled JAR to the SparkContext using the `pyFiles` argument as shown in the [Standalone Programs](python-programming-guide.html#standalone-programs) section of the Spark Python Programming Guide, or you can add dependent JARs one-by-one using SparkContext's `addJar` method. Likewise you can use SparkContext's `addPyFile` method to add `.py`, `.zip` or `.egg` files for distribution.

### Setting Configuration Options
Spark includes several [configuration options](configuration.html#spark-properties)
that influence the behavior of your application.
These are set by building a [SparkConf](api/core/index.html#org.apache.spark.SparkConf)
object and passing it to the SparkContext constructor.

For example, in Java and Scala:

{% highlight scala %}
import org.apache.spark.{SparkConf, SparkContext}
val conf = new SparkConf()
             .setMaster("local")
             .setAppName("My application")
             .set("spark.executor.memory", "1g")
val sc = new SparkContext(conf)
{% endhighlight %}

In Python:

{% highlight scala %}
from pyspark import SparkConf, SparkContext
conf = SparkConf()
conf.setMaster("local")
conf.setAppName("My application")
conf.set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)
{% endhighlight %}

### Accessing Hadoop Filesystems

The examples in this Quick Start access a local file. To read data from a distributed
filesystem, such as HDFS, include 
Hadoop version information in your build file as explained in the [Building](index.html#building) section of Getting Started with Apache Spark.
By default, Spark builds against HDFS 1.0.4.

# Where to Go from Here

The next step for Scala, Java and Python developers is to go to the [Spark Scala Programming Guide](scala-programming-guide.html) to learn more about Spark's key concepts.
