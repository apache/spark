---
layout: global
title: Spark Programming Guide
description: Spark SPARK_VERSION_SHORT programming guide in Java, Scala and Python
---

* This will become a table of contents (this text will be scraped).
{:toc}


# Overview

At a high level, every Spark application consists of a *driver program* that runs the user's `main` function and executes various *parallel operations* on a cluster. The main abstraction Spark provides is a *resilient distributed dataset* (RDD), which is a collection of elements partitioned across the nodes of the cluster that can be operated on in parallel. RDDs are created by starting with a file in the Hadoop file system (or any other Hadoop-supported file system), or an existing Scala collection in the driver program, and transforming it. Users may also ask Spark to *persist* an RDD in memory, allowing it to be reused efficiently across parallel operations. Finally, RDDs automatically recover from node failures.

A second abstraction in Spark is *shared variables* that can be used in parallel operations. By default, when Spark runs a function in parallel as a set of tasks on different nodes, it ships a copy of each variable used in the function to each task. Sometimes, a variable needs to be shared across tasks, or between tasks and the driver program. Spark supports two types of shared variables: *broadcast variables*, which can be used to cache a value in memory on all nodes, and *accumulators*, which are variables that are only "added" to, such as counters and sums.

This guide shows each of these features in each of Spark's supported languages. It is easiest to follow
along with if you launch Spark's interactive shell -- either `bin/spark-shell` for the Scala shell or
`bin/pyspark` for the Python one.

# Linking with Spark

<div class="codetabs">

<div data-lang="scala"  markdown="1">

Spark {{site.SPARK_VERSION}} uses Scala {{site.SCALA_BINARY_VERSION}}. To write
applications in Scala, you will need to use a compatible Scala version (e.g. {{site.SCALA_BINARY_VERSION}}.X).

To write a Spark application, you need to add a Maven dependency on Spark. Spark is available through Maven Central at:

    groupId = org.apache.spark
    artifactId = spark-core_{{site.SCALA_BINARY_VERSION}}
    version = {{site.SPARK_VERSION}}

In addition, if you wish to access an HDFS cluster, you need to add a dependency on
`hadoop-client` for your version of HDFS. Some common HDFS version tags are listed on the
[third party distributions](hadoop-third-party-distributions.html) page.

    groupId = org.apache.hadoop
    artifactId = hadoop-client
    version = <your-hdfs-version>

Finally, you need to import some Spark classes and implicit conversions into your program. Add the following lines:

{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

Spark {{site.SPARK_VERSION}} works with Java 6 and higher. If you are using Java 8, Spark supports
[lambda expressions](http://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html)
for concisely writing functions, otherwise you can use the classes in the
[org.apache.spark.api.java.function](api/java/index.html?org/apache/spark/api/java/function/package-summary.html) package.

To write a Spark application in Java, you need to add a dependency on Spark. Spark is available through Maven Central at:

    groupId = org.apache.spark
    artifactId = spark-core_{{site.SCALA_BINARY_VERSION}}
    version = {{site.SPARK_VERSION}}

In addition, if you wish to access an HDFS cluster, you need to add a dependency on
`hadoop-client` for your version of HDFS. Some common HDFS version tags are listed on the
[third party distributions](hadoop-third-party-distributions.html) page.

    groupId = org.apache.hadoop
    artifactId = hadoop-client
    version = <your-hdfs-version>

Finally, you need to import some Spark classes into your program. Add the following lines:

{% highlight scala %}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkConf
{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

Spark {{site.SPARK_VERSION}} works with Python 2.6 or higher (but not Python 3). It uses the standard CPython interpreter,
so C libraries like NumPy can be used.

To run Spark applications in Python, use the `bin/spark-submit` script located in the Spark directory.
This script will load Spark's Java/Scala libraries and allow you to submit applications to a cluster.
You can also use `bin/pyspark` to launch an interactive Python shell.

If you wish to access HDFS data, you need to use a build of PySpark linking
to your version of HDFS. Some common HDFS version tags are listed on the
[third party distributions](hadoop-third-party-distributions.html) page.
[Prebuilt packages](http://spark.apache.org/downloads.html) are also available on the Spark homepage
for common HDFS versions.

Finally, you need to import some Spark classes into your program. Add the following lines:

{% highlight scala %}
from pyspark import SparkContext, SparkConf
{% endhighlight %}

</div>

</div>


# Initializing Spark

<div class="codetabs">

<div data-lang="scala"  markdown="1">

The first thing a Spark program must do is to create a [SparkContext](api/scala/index.html#org.apache.spark.SparkContext) object, which tells Spark
how to access a cluster. To create a `SparkContext` you first need to build a [SparkConf](api/scala/index.html#org.apache.spark.SparkConf) object
that contains information about your application.

Only one SparkContext may be active per JVM.  You must `stop()` the active SparkContext before creating a new one.

{% highlight scala %}
val conf = new SparkConf().setAppName(appName).setMaster(master)
new SparkContext(conf)
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

The first thing a Spark program must do is to create a [JavaSparkContext](api/java/index.html?org/apache/spark/api/java/JavaSparkContext.html) object, which tells Spark
how to access a cluster. To create a `SparkContext` you first need to build a [SparkConf](api/java/index.html?org/apache/spark/SparkConf.html) object
that contains information about your application.

{% highlight java %}
SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
JavaSparkContext sc = new JavaSparkContext(conf);
{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

The first thing a Spark program must do is to create a [SparkContext](api/python/pyspark.context.SparkContext-class.html) object, which tells Spark
how to access a cluster. To create a `SparkContext` you first need to build a [SparkConf](api/python/pyspark.conf.SparkConf-class.html) object
that contains information about your application.

{% highlight python %}
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
{% endhighlight %}

</div>

</div>

The `appName` parameter is a name for your application to show on the cluster UI.
`master` is a [Spark, Mesos or YARN cluster URL](submitting-applications.html#master-urls),
or a special "local" string to run in local mode.
In practice, when running on a cluster, you will not want to hardcode `master` in the program,
but rather [launch the application with `spark-submit`](submitting-applications.html) and
receive it there. However, for local testing and unit tests, you can pass "local" to run Spark
in-process.


## Using the Shell

<div class="codetabs">

<div data-lang="scala"  markdown="1">

In the Spark shell, a special interpreter-aware SparkContext is already created for you, in the
variable called `sc`. Making your own SparkContext will not work. You can set which master the
context connects to using the `--master` argument, and you can add JARs to the classpath
by passing a comma-separated list to the `--jars` argument.
For example, to run `bin/spark-shell` on exactly four cores, use:

{% highlight bash %}
$ ./bin/spark-shell --master local[4]
{% endhighlight %}

Or, to also add `code.jar` to its classpath, use:

{% highlight bash %}
$ ./bin/spark-shell --master local[4] --jars code.jar
{% endhighlight %}

For a complete list of options, run `spark-shell --help`. Behind the scenes,
`spark-shell` invokes the more general [`spark-submit` script](submitting-applications.html).

</div>

<div data-lang="python"  markdown="1">

In the PySpark shell, a special interpreter-aware SparkContext is already created for you, in the
variable called `sc`. Making your own SparkContext will not work. You can set which master the
context connects to using the `--master` argument, and you can add Python .zip, .egg or .py files
to the runtime path by passing a comma-separated list to `--py-files`.
For example, to run `bin/pyspark` on exactly four cores, use:

{% highlight bash %}
$ ./bin/pyspark --master local[4]
{% endhighlight %}

Or, to also add `code.py` to the search path (in order to later be able to `import code`), use:

{% highlight bash %}
$ ./bin/pyspark --master local[4] --py-files code.py
{% endhighlight %}

For a complete list of options, run `pyspark --help`. Behind the scenes,
`pyspark` invokes the more general [`spark-submit` script](submitting-applications.html).

It is also possible to launch the PySpark shell in [IPython](http://ipython.org), the
enhanced Python interpreter. PySpark works with IPython 1.0.0 and later. To
use IPython, set the `PYSPARK_DRIVER_PYTHON` variable to `ipython` when running `bin/pyspark`:

{% highlight bash %}
$ PYSPARK_DRIVER_PYTHON=ipython ./bin/pyspark
{% endhighlight %}

You can customize the `ipython` command by setting `PYSPARK_DRIVER_PYTHON_OPTS`. For example, to launch
the [IPython Notebook](http://ipython.org/notebook.html) with PyLab plot support:

{% highlight bash %}
$ PYSPARK_DRIVER_PYTHON=ipython PYSPARK_DRIVER_PYTHON_OPTS="notebook --pylab inline" ./bin/pyspark
{% endhighlight %}

</div>

</div>

# Resilient Distributed Datasets (RDDs)

Spark revolves around the concept of a _resilient distributed dataset_ (RDD), which is a fault-tolerant collection of elements that can be operated on in parallel. There are two ways to create RDDs: *parallelizing*
an existing collection in your driver program, or referencing a dataset in an external storage system, such as a
shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.

## Parallelized Collections

<div class="codetabs">

<div data-lang="scala"  markdown="1">

Parallelized collections are created by calling `SparkContext`'s `parallelize` method on an existing collection in your driver program (a Scala `Seq`). The elements of the collection are copied to form a distributed dataset that can be operated on in parallel. For example, here is how to create a parallelized collection holding the numbers 1 to 5:

{% highlight scala %}
val data = Array(1, 2, 3, 4, 5)
val distData = sc.parallelize(data)
{% endhighlight %}

Once created, the distributed dataset (`distData`) can be operated on in parallel. For example, we might call `distData.reduce((a, b) => a + b)` to add up the elements of the array. We describe operations on distributed datasets later on.

</div>

<div data-lang="java"  markdown="1">

Parallelized collections are created by calling `JavaSparkContext`'s `parallelize` method on an existing `Collection` in your driver program. The elements of the collection are copied to form a distributed dataset that can be operated on in parallel. For example, here is how to create a parallelized collection holding the numbers 1 to 5:

{% highlight java %}
List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
JavaRDD<Integer> distData = sc.parallelize(data);
{% endhighlight %}

Once created, the distributed dataset (`distData`) can be operated on in parallel. For example, we might call `distData.reduce((a, b) -> a + b)` to add up the elements of the list.
We describe operations on distributed datasets later on.

**Note:** *In this guide, we'll often use the concise Java 8 lambda syntax to specify Java functions, but
in older versions of Java you can implement the interfaces in the
[org.apache.spark.api.java.function](api/java/index.html?org/apache/spark/api/java/function/package-summary.html) package.
We describe [passing functions to Spark](#passing-functions-to-spark) in more detail below.*

</div>

<div data-lang="python"  markdown="1">

Parallelized collections are created by calling `SparkContext`'s `parallelize` method on an existing iterable or collection in your driver program. The elements of the collection are copied to form a distributed dataset that can be operated on in parallel. For example, here is how to create a parallelized collection holding the numbers 1 to 5:

{% highlight python %}
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
{% endhighlight %}

Once created, the distributed dataset (`distData`) can be operated on in parallel. For example, we can call `distData.reduce(lambda a, b: a + b)` to add up the elements of the list.
We describe operations on distributed datasets later on.

</div>

</div>

One important parameter for parallel collections is the number of *partitions* to cut the dataset into. Spark will run one task for each partition of the cluster. Typically you want 2-4 partitions for each CPU in your cluster. Normally, Spark tries to set the number of partitions automatically based on your cluster. However, you can also set it manually by passing it as a second parameter to `parallelize` (e.g. `sc.parallelize(data, 10)`). Note: some places in the code use the term slices (a synonym for partitions) to maintain backward compatibility.

## External Datasets

<div class="codetabs">

<div data-lang="scala"  markdown="1">

Spark can create distributed datasets from any storage source supported by Hadoop, including your local file system, HDFS, Cassandra, HBase, [Amazon S3](http://wiki.apache.org/hadoop/AmazonS3), etc. Spark supports text files, [SequenceFiles](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/mapred/SequenceFileInputFormat.html), and any other Hadoop [InputFormat](http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapred/InputFormat.html).

Text file RDDs can be created using `SparkContext`'s `textFile` method. This method takes an URI for the file (either a local path on the machine, or a `hdfs://`, `s3n://`, etc URI) and reads it as a collection of lines. Here is an example invocation:

{% highlight scala %}
scala> val distFile = sc.textFile("data.txt")
distFile: RDD[String] = MappedRDD@1d4cee08
{% endhighlight %}

Once created, `distFile` can be acted on by dataset operations. For example, we can add up the sizes of all the lines using the `map` and `reduce` operations as follows: `distFile.map(s => s.length).reduce((a, b) => a + b)`.

Some notes on reading files with Spark:

* If using a path on the local filesystem, the file must also be accessible at the same path on worker nodes. Either copy the file to all workers or use a network-mounted shared file system.

* All of Spark's file-based input methods, including `textFile`, support running on directories, compressed files, and wildcards as well. For example, you can use `textFile("/my/directory")`, `textFile("/my/directory/*.txt")`, and `textFile("/my/directory/*.gz")`.

* The `textFile` method also takes an optional second argument for controlling the number of partitions of the file. By default, Spark creates one partition for each block of the file (blocks being 64MB by default in HDFS), but you can also ask for a higher number of partitions by passing a larger value. Note that you cannot have fewer partitions than blocks.

Apart from text files, Spark's Scala API also supports several other data formats:

* `SparkContext.wholeTextFiles` lets you read a directory containing multiple small text files, and returns each of them as (filename, content) pairs. This is in contrast with `textFile`, which would return one record per line in each file.

* For [SequenceFiles](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/mapred/SequenceFileInputFormat.html), use SparkContext's `sequenceFile[K, V]` method where `K` and `V` are the types of key and values in the file. These should be subclasses of Hadoop's [Writable](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/Writable.html) interface, like [IntWritable](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/IntWritable.html) and [Text](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/Text.html). In addition, Spark allows you to specify native types for a few common Writables; for example, `sequenceFile[Int, String]` will automatically read IntWritables and Texts.

* For other Hadoop InputFormats, you can use the `SparkContext.hadoopRDD` method, which takes an arbitrary `JobConf` and input format class, key class and value class. Set these the same way you would for a Hadoop job with your input source. You can also use `SparkContext.newHadoopRDD` for InputFormats based on the "new" MapReduce API (`org.apache.hadoop.mapreduce`).

* `RDD.saveAsObjectFile` and `SparkContext.objectFile` support saving an RDD in a simple format consisting of serialized Java objects. While this is not as efficient as specialized formats like Avro, it offers an easy way to save any RDD.

</div>

<div data-lang="java"  markdown="1">

Spark can create distributed datasets from any storage source supported by Hadoop, including your local file system, HDFS, Cassandra, HBase, [Amazon S3](http://wiki.apache.org/hadoop/AmazonS3), etc. Spark supports text files, [SequenceFiles](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/mapred/SequenceFileInputFormat.html), and any other Hadoop [InputFormat](http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapred/InputFormat.html).

Text file RDDs can be created using `SparkContext`'s `textFile` method. This method takes an URI for the file (either a local path on the machine, or a `hdfs://`, `s3n://`, etc URI) and reads it as a collection of lines. Here is an example invocation:

{% highlight java %}
JavaRDD<String> distFile = sc.textFile("data.txt");
{% endhighlight %}

Once created, `distFile` can be acted on by dataset operations. For example, we can add up the sizes of all the lines using the `map` and `reduce` operations as follows: `distFile.map(s -> s.length()).reduce((a, b) -> a + b)`.

Some notes on reading files with Spark:

* If using a path on the local filesystem, the file must also be accessible at the same path on worker nodes. Either copy the file to all workers or use a network-mounted shared file system.

* All of Spark's file-based input methods, including `textFile`, support running on directories, compressed files, and wildcards as well. For example, you can use `textFile("/my/directory")`, `textFile("/my/directory/*.txt")`, and `textFile("/my/directory/*.gz")`.

* The `textFile` method also takes an optional second argument for controlling the number of partitions of the file. By default, Spark creates one partition for each block of the file (blocks being 64MB by default in HDFS), but you can also ask for a higher number of partitions by passing a larger value. Note that you cannot have fewer partitions than blocks.

Apart from text files, Spark's Java API also supports several other data formats:

* `JavaSparkContext.wholeTextFiles` lets you read a directory containing multiple small text files, and returns each of them as (filename, content) pairs. This is in contrast with `textFile`, which would return one record per line in each file.

* For [SequenceFiles](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/mapred/SequenceFileInputFormat.html), use SparkContext's `sequenceFile[K, V]` method where `K` and `V` are the types of key and values in the file. These should be subclasses of Hadoop's [Writable](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/Writable.html) interface, like [IntWritable](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/IntWritable.html) and [Text](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/io/Text.html).

* For other Hadoop InputFormats, you can use the `JavaSparkContext.hadoopRDD` method, which takes an arbitrary `JobConf` and input format class, key class and value class. Set these the same way you would for a Hadoop job with your input source. You can also use `JavaSparkContext.newHadoopRDD` for InputFormats based on the "new" MapReduce API (`org.apache.hadoop.mapreduce`).

* `JavaRDD.saveAsObjectFile` and `JavaSparkContext.objectFile` support saving an RDD in a simple format consisting of serialized Java objects. While this is not as efficient as specialized formats like Avro, it offers an easy way to save any RDD.

</div>

<div data-lang="python"  markdown="1">

PySpark can create distributed datasets from any storage source supported by Hadoop, including your local file system, HDFS, Cassandra, HBase, [Amazon S3](http://wiki.apache.org/hadoop/AmazonS3), etc. Spark supports text files, [SequenceFiles](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/mapred/SequenceFileInputFormat.html), and any other Hadoop [InputFormat](http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapred/InputFormat.html).

Text file RDDs can be created using `SparkContext`'s `textFile` method. This method takes an URI for the file (either a local path on the machine, or a `hdfs://`, `s3n://`, etc URI) and reads it as a collection of lines. Here is an example invocation:

{% highlight python %}
>>> distFile = sc.textFile("data.txt")
{% endhighlight %}

Once created, `distFile` can be acted on by dataset operations. For example, we can add up the sizes of all the lines using the `map` and `reduce` operations as follows: `distFile.map(lambda s: len(s)).reduce(lambda a, b: a + b)`.

Some notes on reading files with Spark:

* If using a path on the local filesystem, the file must also be accessible at the same path on worker nodes. Either copy the file to all workers or use a network-mounted shared file system.

* All of Spark's file-based input methods, including `textFile`, support running on directories, compressed files, and wildcards as well. For example, you can use `textFile("/my/directory")`, `textFile("/my/directory/*.txt")`, and `textFile("/my/directory/*.gz")`.

* The `textFile` method also takes an optional second argument for controlling the number of partitions of the file. By default, Spark creates one partition for each block of the file (blocks being 64MB by default in HDFS), but you can also ask for a higher number of partitions by passing a larger value. Note that you cannot have fewer partitions than blocks.

Apart from text files, Spark's Python API also supports several other data formats:

* `SparkContext.wholeTextFiles` lets you read a directory containing multiple small text files, and returns each of them as (filename, content) pairs. This is in contrast with `textFile`, which would return one record per line in each file.

* `RDD.saveAsPickleFile` and `SparkContext.pickleFile` support saving an RDD in a simple format consisting of pickled Python objects. Batching is used on pickle serialization, with default batch size 10.

* SequenceFile and Hadoop Input/Output Formats

**Note** this feature is currently marked ```Experimental``` and is intended for advanced users. It may be replaced in future with read/write support based on Spark SQL, in which case Spark SQL is the preferred approach.

**Writable Support**

PySpark SequenceFile support loads an RDD of key-value pairs within Java, converts Writables to base Java types, and pickles the 
resulting Java objects using [Pyrolite](https://github.com/irmen/Pyrolite/). When saving an RDD of key-value pairs to SequenceFile, 
PySpark does the reverse. It unpickles Python objects into Java objects and then converts them to Writables. The following 
Writables are automatically converted:

<table class="table">
<tr><th>Writable Type</th><th>Python Type</th></tr>
<tr><td>Text</td><td>unicode str</td></tr>
<tr><td>IntWritable</td><td>int</td></tr>
<tr><td>FloatWritable</td><td>float</td></tr>
<tr><td>DoubleWritable</td><td>float</td></tr>
<tr><td>BooleanWritable</td><td>bool</td></tr>
<tr><td>BytesWritable</td><td>bytearray</td></tr>
<tr><td>NullWritable</td><td>None</td></tr>
<tr><td>MapWritable</td><td>dict</td></tr>
</table>

Arrays are not handled out-of-the-box. Users need to specify custom `ArrayWritable` subtypes when reading or writing. When writing, 
users also need to specify custom converters that convert arrays to custom `ArrayWritable` subtypes. When reading, the default 
converter will convert custom `ArrayWritable` subtypes to Java `Object[]`, which then get pickled to Python tuples. To get 
Python `array.array` for arrays of primitive types, users need to specify custom converters.

**Saving and Loading SequenceFiles**

Similarly to text files, SequenceFiles can be saved and loaded by specifying the path. The key and value
classes can be specified, but for standard Writables this is not required.

{% highlight python %}
>>> rdd = sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x ))
>>> rdd.saveAsSequenceFile("path/to/file")
>>> sorted(sc.sequenceFile("path/to/file").collect())
[(1, u'a'), (2, u'aa'), (3, u'aaa')]
{% endhighlight %}

**Saving and Loading Other Hadoop Input/Output Formats**

PySpark can also read any Hadoop InputFormat or write any Hadoop OutputFormat, for both 'new' and 'old' Hadoop MapReduce APIs. 
If required, a Hadoop configuration can be passed in as a Python dict. Here is an example using the
Elasticsearch ESInputFormat:

{% highlight python %}
$ SPARK_CLASSPATH=/path/to/elasticsearch-hadoop.jar ./bin/pyspark
>>> conf = {"es.resource" : "index/type"}   # assume Elasticsearch is running on localhost defaults
>>> rdd = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)
>>> rdd.first()         # the result is a MapWritable that is converted to a Python dict
(u'Elasticsearch ID',
 {u'field1': True,
  u'field2': u'Some Text',
  u'field3': 12345})
{% endhighlight %}

Note that, if the InputFormat simply depends on a Hadoop configuration and/or input path, and
the key and value classes can easily be converted according to the above table,
then this approach should work well for such cases.

If you have custom serialized binary data (such as loading data from Cassandra / HBase), then you will first need to 
transform that data on the Scala/Java side to something which can be handled by Pyrolite's pickler.
A [Converter](api/scala/index.html#org.apache.spark.api.python.Converter) trait is provided 
for this. Simply extend this trait and implement your transformation code in the ```convert``` 
method. Remember to ensure that this class, along with any dependencies required to access your ```InputFormat```, are packaged into your Spark job jar and included on the PySpark 
classpath.

See the [Python examples]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/python) and 
the [Converter examples]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/scala/org/apache/spark/examples/pythonconverters) 
for examples of using Cassandra / HBase ```InputFormat``` and ```OutputFormat``` with custom converters.

</div>

</div>

## RDD Operations

RDDs support two types of operations: *transformations*, which create a new dataset from an existing one, and *actions*, which return a value to the driver program after running a computation on the dataset. For example, `map` is a transformation that passes each dataset element through a function and returns a new RDD representing the results. On the other hand, `reduce` is an action that aggregates all the elements of the RDD using some function and returns the final result to the driver program (although there is also a parallel `reduceByKey` that returns a distributed dataset).

All transformations in Spark are <i>lazy</i>, in that they do not compute their results right away. Instead, they just remember the transformations applied to some base dataset (e.g. a file). The transformations are only computed when an action requires a result to be returned to the driver program. This design enables Spark to run more efficiently -- for example, we can realize that a dataset created through `map` will be used in a `reduce` and return only the result of the `reduce` to the driver, rather than the larger mapped dataset.

By default, each transformed RDD may be recomputed each time you run an action on it. However, you may also *persist* an RDD in memory using the `persist` (or `cache`) method, in which case Spark will keep the elements around on the cluster for much faster access the next time you query it. There is also support for persisting RDDs on disk, or replicated across multiple nodes.

### Basics

<div class="codetabs">

<div data-lang="scala" markdown="1">

To illustrate RDD basics, consider the simple program below:

{% highlight scala %}
val lines = sc.textFile("data.txt")
val lineLengths = lines.map(s => s.length)
val totalLength = lineLengths.reduce((a, b) => a + b)
{% endhighlight %}

The first line defines a base RDD from an external file. This dataset is not loaded in memory or
otherwise acted on: `lines` is merely a pointer to the file.
The second line defines `lineLengths` as the result of a `map` transformation. Again, `lineLengths`
is *not* immediately computed, due to laziness.
Finally, we run `reduce`, which is an action. At this point Spark breaks the computation into tasks
to run on separate machines, and each machine runs both its part of the map and a local reduction,
returning only its answer to the driver program.

If we also wanted to use `lineLengths` again later, we could add:

{% highlight scala %}
lineLengths.persist()
{% endhighlight %}

before the `reduce`, which would cause `lineLengths` to be saved in memory after the first time it is computed.

</div>

<div data-lang="java" markdown="1">

To illustrate RDD basics, consider the simple program below:

{% highlight java %}
JavaRDD<String> lines = sc.textFile("data.txt");
JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
int totalLength = lineLengths.reduce((a, b) -> a + b);
{% endhighlight %}

The first line defines a base RDD from an external file. This dataset is not loaded in memory or
otherwise acted on: `lines` is merely a pointer to the file.
The second line defines `lineLengths` as the result of a `map` transformation. Again, `lineLengths`
is *not* immediately computed, due to laziness.
Finally, we run `reduce`, which is an action. At this point Spark breaks the computation into tasks
to run on separate machines, and each machine runs both its part of the map and a local reduction,
returning only its answer to the driver program.

If we also wanted to use `lineLengths` again later, we could add:

{% highlight java %}
lineLengths.persist();
{% endhighlight %}

before the `reduce`, which would cause `lineLengths` to be saved in memory after the first time it is computed.

</div>

<div data-lang="python" markdown="1">

To illustrate RDD basics, consider the simple program below:

{% highlight python %}
lines = sc.textFile("data.txt")
lineLengths = lines.map(lambda s: len(s))
totalLength = lineLengths.reduce(lambda a, b: a + b)
{% endhighlight %}

The first line defines a base RDD from an external file. This dataset is not loaded in memory or
otherwise acted on: `lines` is merely a pointer to the file.
The second line defines `lineLengths` as the result of a `map` transformation. Again, `lineLengths`
is *not* immediately computed, due to laziness.
Finally, we run `reduce`, which is an action. At this point Spark breaks the computation into tasks
to run on separate machines, and each machine runs both its part of the map and a local reduction,
returning only its answer to the driver program.

If we also wanted to use `lineLengths` again later, we could add:

{% highlight python %}
lineLengths.persist()
{% endhighlight %}

before the `reduce`, which would cause `lineLengths` to be saved in memory after the first time it is computed.

</div>

</div>

### Passing Functions to Spark

<div class="codetabs">

<div data-lang="scala" markdown="1">

Spark's API relies heavily on passing functions in the driver program to run on the cluster.
There are two recommended ways to do this:

* [Anonymous function syntax](http://docs.scala-lang.org/tutorials/tour/anonymous-function-syntax.html),
  which can be used for short pieces of code.
* Static methods in a global singleton object. For example, you can define `object MyFunctions` and then
  pass `MyFunctions.func1`, as follows:

{% highlight scala %}
object MyFunctions {
  def func1(s: String): String = { ... }
}

myRdd.map(MyFunctions.func1)
{% endhighlight %}

Note that while it is also possible to pass a reference to a method in a class instance (as opposed to
a singleton object), this requires sending the object that contains that class along with the method.
For example, consider:

{% highlight scala %}
class MyClass {
  def func1(s: String): String = { ... }
  def doStuff(rdd: RDD[String]): RDD[String] = { rdd.map(func1) }
}
{% endhighlight %}

Here, if we create a `new MyClass` and call `doStuff` on it, the `map` inside there references the
`func1` method *of that `MyClass` instance*, so the whole object needs to be sent to the cluster. It is
similar to writing `rdd.map(x => this.func1(x))`.

In a similar way, accessing fields of the outer object will reference the whole object:

{% highlight scala %}
class MyClass {
  val field = "Hello"
  def doStuff(rdd: RDD[String]): RDD[String] = { rdd.map(x => field + x) }
}
{% endhighlight %}

is equilvalent to writing `rdd.map(x => this.field + x)`, which references all of `this`. To avoid this
issue, the simplest way is to copy `field` into a local variable instead of accessing it externally:

{% highlight scala %}
def doStuff(rdd: RDD[String]): RDD[String] = {
  val field_ = this.field
  rdd.map(x => field_ + x)
}
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

Spark's API relies heavily on passing functions in the driver program to run on the cluster.
In Java, functions are represented by classes implementing the interfaces in the
[org.apache.spark.api.java.function](api/java/index.html?org/apache/spark/api/java/function/package-summary.html) package.
There are two ways to create such functions:

* Implement the Function interfaces in your own class, either as an anonymous inner class or a named one,
  and pass an instance of it to Spark.
* In Java 8, use [lambda expressions](http://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html)
  to concisely define an implementation.

While much of this guide uses lambda syntax for conciseness, it is easy to use all the same APIs
in long-form. For example, we could have written our code above as follows:

{% highlight java %}
JavaRDD<String> lines = sc.textFile("data.txt");
JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
  public Integer call(String s) { return s.length(); }
});
int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
  public Integer call(Integer a, Integer b) { return a + b; }
});
{% endhighlight %}

Or, if writing the functions inline is unwieldy:

{% highlight java %}
class GetLength implements Function<String, Integer> {
  public Integer call(String s) { return s.length(); }
}
class Sum implements Function2<Integer, Integer, Integer> {
  public Integer call(Integer a, Integer b) { return a + b; }
}

JavaRDD<String> lines = sc.textFile("data.txt");
JavaRDD<Integer> lineLengths = lines.map(new GetLength());
int totalLength = lineLengths.reduce(new Sum());
{% endhighlight %}

Note that anonymous inner classes in Java can also access variables in the enclosing scope as long
as they are marked `final`. Spark will ship copies of these variables to each worker node as it does
for other languages.

</div>

<div data-lang="python"  markdown="1">

Spark's API relies heavily on passing functions in the driver program to run on the cluster.
There are three recommended ways to do this:

* [Lambda expressions](https://docs.python.org/2/tutorial/controlflow.html#lambda-expressions),
  for simple functions that can be written as an expression. (Lambdas do not support multi-statement
  functions or statements that do not return a value.)
* Local `def`s inside the function calling into Spark, for longer code.
* Top-level functions in a module.

For example, to pass a longer function than can be supported using a `lambda`, consider
the code below:

{% highlight python %}
"""MyScript.py"""
if __name__ == "__main__":
    def myFunc(s):
        words = s.split(" ")
        return len(words)

    sc = SparkContext(...)
    sc.textFile("file.txt").map(myFunc)
{% endhighlight %}

Note that while it is also possible to pass a reference to a method in a class instance (as opposed to
a singleton object), this requires sending the object that contains that class along with the method.
For example, consider:

{% highlight python %}
class MyClass(object):
    def func(self, s):
        return s
    def doStuff(self, rdd):
        return rdd.map(self.func)
{% endhighlight %}

Here, if we create a `new MyClass` and call `doStuff` on it, the `map` inside there references the
`func` method *of that `MyClass` instance*, so the whole object needs to be sent to the cluster.

In a similar way, accessing fields of the outer object will reference the whole object:

{% highlight python %}
class MyClass(object):
    def __init__(self):
        self.field = "Hello"
    def doStuff(self, rdd):
        return rdd.map(lambda s: self.field + x)
{% endhighlight %}

To avoid this issue, the simplest way is to copy `field` into a local variable instead
of accessing it externally:

{% highlight python %}
def doStuff(self, rdd):
    field = self.field
    return rdd.map(lambda s: field + x)
{% endhighlight %}

</div>

</div>

### Working with Key-Value Pairs

<div class="codetabs">

<div data-lang="scala" markdown="1">

While most Spark operations work on RDDs containing any type of objects, a few special operations are
only available on RDDs of key-value pairs.
The most common ones are distributed "shuffle" operations, such as grouping or aggregating the elements
by a key.

In Scala, these operations are automatically available on RDDs containing
[Tuple2](http://www.scala-lang.org/api/{{site.SCALA_VERSION}}/index.html#scala.Tuple2) objects
(the built-in tuples in the language, created by simply writing `(a, b)`), as long as you
import `org.apache.spark.SparkContext._` in your program to enable Spark's implicit
conversions. The key-value pair operations are available in the
[PairRDDFunctions](api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions) class,
which automatically wraps around an RDD of tuples if you import the conversions.

For example, the following code uses the `reduceByKey` operation on key-value pairs to count how
many times each line of text occurs in a file:

{% highlight scala %}
val lines = sc.textFile("data.txt")
val pairs = lines.map(s => (s, 1))
val counts = pairs.reduceByKey((a, b) => a + b)
{% endhighlight %}

We could also use `counts.sortByKey()`, for example, to sort the pairs alphabetically, and finally
`counts.collect()` to bring them back to the driver program as an array of objects.

**Note:** when using custom objects as the key in key-value pair operations, you must be sure that a
custom `equals()` method is accompanied with a matching `hashCode()` method.  For full details, see
the contract outlined in the [Object.hashCode()
documentation](http://docs.oracle.com/javase/7/docs/api/java/lang/Object.html#hashCode()).

</div>

<div data-lang="java" markdown="1">

While most Spark operations work on RDDs containing any type of objects, a few special operations are
only available on RDDs of key-value pairs.
The most common ones are distributed "shuffle" operations, such as grouping or aggregating the elements
by a key.

In Java, key-value pairs are represented using the 
[scala.Tuple2](http://www.scala-lang.org/api/{{site.SCALA_VERSION}}/index.html#scala.Tuple2) class
from the Scala standard library. You can simply call `new Tuple2(a, b)` to create a tuple, and access
its fields later with `tuple._1()` and `tuple._2()`.

RDDs of key-value pairs are represented by the
[JavaPairRDD](api/java/index.html?org/apache/spark/api/java/JavaPairRDD.html) class. You can construct
JavaPairRDDs from JavaRDDs using special versions of the `map` operations, like
`mapToPair` and `flatMapToPair`. The JavaPairRDD will have both standard RDD functions and special
key-value ones.

For example, the following code uses the `reduceByKey` operation on key-value pairs to count how
many times each line of text occurs in a file:

{% highlight scala %}
JavaRDD<String> lines = sc.textFile("data.txt");
JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
{% endhighlight %}

We could also use `counts.sortByKey()`, for example, to sort the pairs alphabetically, and finally
`counts.collect()` to bring them back to the driver program as an array of objects.

**Note:** when using custom objects as the key in key-value pair operations, you must be sure that a
custom `equals()` method is accompanied with a matching `hashCode()` method.  For full details, see
the contract outlined in the [Object.hashCode()
documentation](http://docs.oracle.com/javase/7/docs/api/java/lang/Object.html#hashCode()).

</div>

<div data-lang="python" markdown="1">

While most Spark operations work on RDDs containing any type of objects, a few special operations are
only available on RDDs of key-value pairs.
The most common ones are distributed "shuffle" operations, such as grouping or aggregating the elements
by a key.

In Python, these operations work on RDDs containing built-in Python tuples such as `(1, 2)`.
Simply create such tuples and then call your desired operation.

For example, the following code uses the `reduceByKey` operation on key-value pairs to count how
many times each line of text occurs in a file:

{% highlight python %}
lines = sc.textFile("data.txt")
pairs = lines.map(lambda s: (s, 1))
counts = pairs.reduceByKey(lambda a, b: a + b)
{% endhighlight %}

We could also use `counts.sortByKey()`, for example, to sort the pairs alphabetically, and finally
`counts.collect()` to bring them back to the driver program as a list of objects.

</div>

</div>


### Transformations

The following table lists some of the common transformations supported by Spark. Refer to the
RDD API doc
([Scala](api/scala/index.html#org.apache.spark.rdd.RDD),
 [Java](api/java/index.html?org/apache/spark/api/java/JavaRDD.html),
 [Python](api/python/pyspark.rdd.RDD-class.html))
and pair RDD functions doc
([Scala](api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions),
 [Java](api/java/index.html?org/apache/spark/api/java/JavaPairRDD.html))
for details.

<table class="table">
<tr><th style="width:25%">Transformation</th><th>Meaning</th></tr>
<tr>
  <td> <b>map</b>(<i>func</i>) </td>
  <td> Return a new distributed dataset formed by passing each element of the source through a function <i>func</i>. </td>
</tr>
<tr>
  <td> <b>filter</b>(<i>func</i>) </td>
  <td> Return a new dataset formed by selecting those elements of the source on which <i>func</i> returns true. </td>
</tr>
<tr>
  <td> <b>flatMap</b>(<i>func</i>) </td>
  <td> Similar to map, but each input item can be mapped to 0 or more output items (so <i>func</i> should return a Seq rather than a single item). </td>
</tr>
<tr>
  <td> <b>mapPartitions</b>(<i>func</i>) </td>
  <td> Similar to map, but runs separately on each partition (block) of the RDD, so <i>func</i> must be of type
    Iterator&lt;T&gt; => Iterator&lt;U&gt; when running on an RDD of type T. </td>
</tr>
<tr>
  <td> <b>mapPartitionsWithIndex</b>(<i>func</i>) </td>
  <td> Similar to mapPartitions, but also provides <i>func</i> with an integer value representing the index of
  the partition, so <i>func</i> must be of type (Int, Iterator&lt;T&gt;) => Iterator&lt;U&gt; when running on an RDD of type T.
  </td>
</tr>
<tr>
  <td> <b>sample</b>(<i>withReplacement</i>, <i>fraction</i>, <i>seed</i>) </td>
  <td> Sample a fraction <i>fraction</i> of the data, with or without replacement, using a given random number generator seed. </td>
</tr>
<tr>
  <td> <b>union</b>(<i>otherDataset</i>) </td>
  <td> Return a new dataset that contains the union of the elements in the source dataset and the argument. </td>
</tr>
<tr>
  <td> <b>intersection</b>(<i>otherDataset</i>) </td>
  <td> Return a new RDD that contains the intersection of elements in the source dataset and the argument. </td>
</tr>
<tr>
  <td> <b>distinct</b>([<i>numTasks</i>])) </td>
  <td> Return a new dataset that contains the distinct elements of the source dataset.</td>
</tr>
<tr>
  <td> <b>groupByKey</b>([<i>numTasks</i>]) </td>
  <td> When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable&lt;V&gt;) pairs. <br />
    <b>Note:</b> If you are grouping in order to perform an aggregation (such as a sum or
      average) over each key, using <code>reduceByKey</code> or <code>aggregateByKey</code> will yield much better 
      performance.
    <br />
    <b>Note:</b> By default, the level of parallelism in the output depends on the number of partitions of the parent RDD.
      You can pass an optional <code>numTasks</code> argument to set a different number of tasks.
  </td>
</tr>
<tr>
  <td> <b>reduceByKey</b>(<i>func</i>, [<i>numTasks</i>]) </td>
  <td> When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function <i>func</i>, which must be of type (V,V) => V. Like in <code>groupByKey</code>, the number of reduce tasks is configurable through an optional second argument. </td>
</tr>
<tr>
  <td> <b>aggregateByKey</b>(<i>zeroValue</i>)(<i>seqOp</i>, <i>combOp</i>, [<i>numTasks</i>]) </td>
  <td> When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while avoiding unnecessary allocations. Like in <code>groupByKey</code>, the number of reduce tasks is configurable through an optional second argument. </td>
</tr>
<tr>
  <td> <b>sortByKey</b>([<i>ascending</i>], [<i>numTasks</i>]) </td>
  <td> When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified in the boolean <code>ascending</code> argument.</td>
</tr>
<tr>
  <td> <b>join</b>(<i>otherDataset</i>, [<i>numTasks</i>]) </td>
  <td> When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key.
    Outer joins are supported through <code>leftOuterJoin</code>, <code>rightOuterJoin</code>, and <code>fullOuterJoin</code>.
  </td>
</tr>
<tr>
  <td> <b>cogroup</b>(<i>otherDataset</i>, [<i>numTasks</i>]) </td>
  <td> When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable&lt;V&gt;, Iterable&lt;W&gt;)) tuples. This operation is also called <code>groupWith</code>. </td>
</tr>
<tr>
  <td> <b>cartesian</b>(<i>otherDataset</i>) </td>
  <td> When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements). </td>
</tr>
<tr>
  <td> <b>pipe</b>(<i>command</i>, <i>[envVars]</i>) </td>
  <td> Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script. RDD elements are written to the
    process's stdin and lines output to its stdout are returned as an RDD of strings. </td>
</tr>
<tr>
  <td> <b>coalesce</b>(<i>numPartitions</i>) </td>
  <td> Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently
    after filtering down a large dataset. </td>
</tr>
<tr>
  <td> <b>repartition</b>(<i>numPartitions</i>) </td>
  <td> Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them.
    This always shuffles all data over the network. </td>
</tr>
<tr>
  <td> <b>repartitionAndSortWithinPartitions</b>(<i>partitioner</i>) </td>
  <td> Repartition the RDD according to the given partitioner and, within each resulting partition,
  sort records by their keys. This is more efficient than calling <code>repartition</code> and then sorting within 
  each partition because it can push the sorting down into the shuffle machinery. </td>
</tr>
</table>

### Actions

The following table lists some of the common actions supported by Spark. Refer to the
RDD API doc
([Scala](api/scala/index.html#org.apache.spark.rdd.RDD),
 [Java](api/java/index.html?org/apache/spark/api/java/JavaRDD.html),
 [Python](api/python/pyspark.rdd.RDD-class.html))
and pair RDD functions doc
([Scala](api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions),
 [Java](api/java/index.html?org/apache/spark/api/java/JavaPairRDD.html))
for details.

<table class="table">
<tr><th>Action</th><th>Meaning</th></tr>
<tr>
  <td> <b>reduce</b>(<i>func</i>) </td>
  <td> Aggregate the elements of the dataset using a function <i>func</i> (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel. </td>
</tr>
<tr>
  <td> <b>collect</b>() </td>
  <td> Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data. </td>
</tr>
<tr>
  <td> <b>count</b>() </td>
  <td> Return the number of elements in the dataset. </td>
</tr>
<tr>
  <td> <b>first</b>() </td>
  <td> Return the first element of the dataset (similar to take(1)). </td>
</tr>
<tr>
  <td> <b>take</b>(<i>n</i>) </td>
  <td> Return an array with the first <i>n</i> elements of the dataset. Note that this is currently not executed in parallel. Instead, the driver program computes all the elements. </td>
</tr>
<tr>
  <td> <b>takeSample</b>(<i>withReplacement</i>, <i>num</i>, [<i>seed</i>]) </td>
  <td> Return an array with a random sample of <i>num</i> elements of the dataset, with or without replacement, optionally pre-specifying a random number generator seed.</td>
</tr>
<tr>
  <td> <b>takeOrdered</b>(<i>n</i>, <i>[ordering]</i>) </td>
  <td> Return the first <i>n</i> elements of the RDD using either their natural order or a custom comparator. </td>
</tr>
<tr>
  <td> <b>saveAsTextFile</b>(<i>path</i>) </td>
  <td> Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file. </td>
</tr>
<tr>
  <td> <b>saveAsSequenceFile</b>(<i>path</i>) <br /> (Java and Scala) </td>
  <td> Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, HDFS or any other Hadoop-supported file system. This is available on RDDs of key-value pairs that either implement Hadoop's Writable interface. In Scala, it is also
   available on types that are implicitly convertible to Writable (Spark includes conversions for basic types like Int, Double, String, etc). </td>
</tr>
<tr>
  <td> <b>saveAsObjectFile</b>(<i>path</i>) <br /> (Java and Scala) </td>
  <td> Write the elements of the dataset in a simple format using Java serialization, which can then be loaded using
    <code>SparkContext.objectFile()</code>. </td>
</tr>
<tr>
  <td> <b>countByKey</b>() </td>
  <td> Only available on RDDs of type (K, V). Returns a hashmap of (K, Int) pairs with the count of each key. </td>
</tr>
<tr>
  <td> <b>foreach</b>(<i>func</i>) </td>
  <td> Run a function <i>func</i> on each element of the dataset. This is usually done for side effects such as updating an accumulator variable (see below) or interacting with external storage systems. </td>
</tr>
</table>

## RDD Persistence

One of the most important capabilities in Spark is *persisting* (or *caching*) a dataset in memory
across operations. When you persist an RDD, each node stores any partitions of it that it computes in
memory and reuses them in other actions on that dataset (or datasets derived from it). This allows
future actions to be much faster (often by more than 10x). Caching is a key tool for
iterative algorithms and fast interactive use.

You can mark an RDD to be persisted using the `persist()` or `cache()` methods on it. The first time
it is computed in an action, it will be kept in memory on the nodes. Spark's cache is fault-tolerant --
if any partition of an RDD is lost, it will automatically be recomputed using the transformations
that originally created it.

In addition, each persisted RDD can be stored using a different *storage level*, allowing you, for example,
to persist the dataset on disk, persist it in memory but as serialized Java objects (to save space),
replicate it across nodes, or store it off-heap in [Tachyon](http://tachyon-project.org/).
These levels are set by passing a
`StorageLevel` object ([Scala](api/scala/index.html#org.apache.spark.storage.StorageLevel),
[Java](api/java/index.html?org/apache/spark/storage/StorageLevel.html),
[Python](api/python/pyspark.storagelevel.StorageLevel-class.html))
to `persist()`. The `cache()` method is a shorthand for using the default storage level,
which is `StorageLevel.MEMORY_ONLY` (store deserialized objects in memory). The full set of
storage levels is:

<table class="table">
<tr><th style="width:23%">Storage Level</th><th>Meaning</th></tr>
<tr>
  <td> MEMORY_ONLY </td>
  <td> Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, some partitions will
    not be cached and will be recomputed on the fly each time they're needed. This is the default level. </td>
</tr>
<tr>
  <td> MEMORY_AND_DISK </td>
  <td> Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, store the
    partitions that don't fit on disk, and read them from there when they're needed. </td>
</tr>
<tr>
  <td> MEMORY_ONLY_SER </td>
  <td> Store RDD as <i>serialized</i> Java objects (one byte array per partition).
    This is generally more space-efficient than deserialized objects, especially when using a
    <a href="tuning.html">fast serializer</a>, but more CPU-intensive to read.
  </td>
</tr>
<tr>
  <td> MEMORY_AND_DISK_SER </td>
  <td> Similar to MEMORY_ONLY_SER, but spill partitions that don't fit in memory to disk instead of
    recomputing them on the fly each time they're needed. </td>
</tr>
<tr>
  <td> DISK_ONLY </td>
  <td> Store the RDD partitions only on disk. </td>
</tr>
<tr>
  <td> MEMORY_ONLY_2, MEMORY_AND_DISK_2, etc.  </td>
  <td> Same as the levels above, but replicate each partition on two cluster nodes. </td>
</tr>
<tr>
  <td> OFF_HEAP (experimental) </td>
  <td> Store RDD in serialized format in <a href="http://tachyon-project.org">Tachyon</a>.
    Compared to MEMORY_ONLY_SER, OFF_HEAP reduces garbage collection overhead and allows executors
    to be smaller and to share a pool of memory, making it attractive in environments with
    large heaps or multiple concurrent applications. Furthermore, as the RDDs reside in Tachyon,
    the crash of an executor does not lead to losing the in-memory cache. In this mode, the memory 
    in Tachyon is discardable. Thus, Tachyon does not attempt to reconstruct a block that it evicts
    from memory.
  </td>
</tr>
</table>

**Note:** *In Python, stored objects will always be serialized with the [Pickle](https://docs.python.org/2/library/pickle.html) library, so it does not matter whether you choose a serialized level.*

Spark also automatically persists some intermediate data in shuffle operations (e.g. `reduceByKey`), even without users calling `persist`. This is done to avoid recomputing the entire input if a node fails during the shuffle. We still recommend users call `persist` on the resulting RDD if they plan to reuse it.

### Which Storage Level to Choose?

Spark's storage levels are meant to provide different trade-offs between memory usage and CPU
efficiency. We recommend going through the following process to select one:

* If your RDDs fit comfortably with the default storage level (`MEMORY_ONLY`), leave them that way.
  This is the most CPU-efficient option, allowing operations on the RDDs to run as fast as possible.

* If not, try using `MEMORY_ONLY_SER` and [selecting a fast serialization library](tuning.html) to
make the objects much more space-efficient, but still reasonably fast to access. 

* Don't spill to disk unless the functions that computed your datasets are expensive, or they filter
a large amount of the data. Otherwise, recomputing a partition may be as fast as reading it from
disk.

* Use the replicated storage levels if you want fast fault recovery (e.g. if using Spark to serve
requests from a web application). *All* the storage levels provide full fault tolerance by
recomputing lost data, but the replicated ones let you continue running tasks on the RDD without
waiting to recompute a lost partition.

* In environments with high amounts of memory or multiple applications, the experimental `OFF_HEAP`
mode has several advantages:
   * It allows multiple executors to share the same pool of memory in Tachyon.
   * It significantly reduces garbage collection costs.
   * Cached data is not lost if individual executors crash.

### Removing Data

Spark automatically monitors cache usage on each node and drops out old data partitions in a
least-recently-used (LRU) fashion. If you would like to manually remove an RDD instead of waiting for
it to fall out of the cache, use the `RDD.unpersist()` method.

# Shared Variables

Normally, when a function passed to a Spark operation (such as `map` or `reduce`) is executed on a
remote cluster node, it works on separate copies of all the variables used in the function. These
variables are copied to each machine, and no updates to the variables on the remote machine are
propagated back to the driver program. Supporting general, read-write shared variables across tasks
would be inefficient. However, Spark does provide two limited types of *shared variables* for two
common usage patterns: broadcast variables and accumulators.

## Broadcast Variables

Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather
than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a
large input dataset in an efficient manner. Spark also attempts to distribute broadcast variables
using efficient broadcast algorithms to reduce communication cost.

Broadcast variables are created from a variable `v` by calling `SparkContext.broadcast(v)`. The
broadcast variable is a wrapper around `v`, and its value can be accessed by calling the `value`
method. The code below shows this:

<div class="codetabs">

<div data-lang="scala"  markdown="1">

{% highlight scala %}
scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
broadcastVar: org.apache.spark.broadcast.Broadcast[Array[Int]] = Broadcast(0)

scala> broadcastVar.value
res0: Array[Int] = Array(1, 2, 3)
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}
Broadcast<int[]> broadcastVar = sc.broadcast(new int[] {1, 2, 3});

broadcastVar.value();
// returns [1, 2, 3]
{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

{% highlight python %}
>>> broadcastVar = sc.broadcast([1, 2, 3])
<pyspark.broadcast.Broadcast object at 0x102789f10>

>>> broadcastVar.value
[1, 2, 3]
{% endhighlight %}

</div>

</div>

After the broadcast variable is created, it should be used instead of the value `v` in any functions
run on the cluster so that `v` is not shipped to the nodes more than once. In addition, the object
`v` should not be modified after it is broadcast in order to ensure that all nodes get the same
value of the broadcast variable (e.g. if the variable is shipped to a new node later).

## Accumulators

Accumulators are variables that are only "added" to through an associative operation and can
therefore be efficiently supported in parallel. They can be used to implement counters (as in
MapReduce) or sums. Spark natively supports accumulators of numeric types, and programmers
can add support for new types. If accumulators are created with a name, they will be
displayed in Spark's UI. This can be useful for understanding the progress of 
running stages (NOTE: this is not yet supported in Python).

An accumulator is created from an initial value `v` by calling `SparkContext.accumulator(v)`. Tasks
running on the cluster can then add to it using the `add` method or the `+=` operator (in Scala and Python).
However, they cannot read its value.
Only the driver program can read the accumulator's value, using its `value` method.

The code below shows an accumulator being used to add up the elements of an array:

<div class="codetabs">

<div data-lang="scala"  markdown="1">

{% highlight scala %}
scala> val accum = sc.accumulator(0, "My Accumulator")
accum: spark.Accumulator[Int] = 0

scala> sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x)
...
10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s

scala> accum.value
res2: Int = 10
{% endhighlight %}

While this code used the built-in support for accumulators of type Int, programmers can also
create their own types by subclassing [AccumulatorParam](api/scala/index.html#org.apache.spark.AccumulatorParam).
The AccumulatorParam interface has two methods: `zero` for providing a "zero value" for your data
type, and `addInPlace` for adding two values together. For example, supposing we had a `Vector` class
representing mathematical vectors, we could write:

{% highlight scala %}
object VectorAccumulatorParam extends AccumulatorParam[Vector] {
  def zero(initialValue: Vector): Vector = {
    Vector.zeros(initialValue.size)
  }
  def addInPlace(v1: Vector, v2: Vector): Vector = {
    v1 += v2
  }
}

// Then, create an Accumulator of this type:
val vecAccum = sc.accumulator(new Vector(...))(VectorAccumulatorParam)
{% endhighlight %}

In Scala, Spark also supports the more general [Accumulable](api/scala/index.html#org.apache.spark.Accumulable)
interface to accumulate data where the resulting type is not the same as the elements added (e.g. build
a list by collecting together elements), and the `SparkContext.accumulableCollection` method for accumulating
common Scala collection types.

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}
Accumulator<Integer> accum = sc.accumulator(0);

sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));
// ...
// 10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s

accum.value();
// returns 10
{% endhighlight %}

While this code used the built-in support for accumulators of type Integer, programmers can also
create their own types by subclassing [AccumulatorParam](api/java/index.html?org/apache/spark/AccumulatorParam.html).
The AccumulatorParam interface has two methods: `zero` for providing a "zero value" for your data
type, and `addInPlace` for adding two values together. For example, supposing we had a `Vector` class
representing mathematical vectors, we could write:

{% highlight java %}
class VectorAccumulatorParam implements AccumulatorParam<Vector> {
  public Vector zero(Vector initialValue) {
    return Vector.zeros(initialValue.size());
  }
  public Vector addInPlace(Vector v1, Vector v2) {
    v1.addInPlace(v2); return v1;
  }
}

// Then, create an Accumulator of this type:
Accumulator<Vector> vecAccum = sc.accumulator(new Vector(...), new VectorAccumulatorParam());
{% endhighlight %}

In Java, Spark also supports the more general [Accumulable](api/java/index.html?org/apache/spark/Accumulable.html)
interface to accumulate data where the resulting type is not the same as the elements added (e.g. build
a list by collecting together elements).

</div>

<div data-lang="python"  markdown="1">

{% highlight python %}
>>> accum = sc.accumulator(0)
Accumulator<id=0, value=0>

>>> sc.parallelize([1, 2, 3, 4]).foreach(lambda x: accum.add(x))
...
10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s

scala> accum.value
10
{% endhighlight %}

While this code used the built-in support for accumulators of type Int, programmers can also
create their own types by subclassing [AccumulatorParam](api/python/pyspark.accumulators.AccumulatorParam-class.html).
The AccumulatorParam interface has two methods: `zero` for providing a "zero value" for your data
type, and `addInPlace` for adding two values together. For example, supposing we had a `Vector` class
representing mathematical vectors, we could write:

{% highlight python %}
class VectorAccumulatorParam(AccumulatorParam):
    def zero(self, initialValue):
        return Vector.zeros(initialValue.size)

    def addInPlace(self, v1, v2):
        v1 += v2
        return v1

# Then, create an Accumulator of this type:
vecAccum = sc.accumulator(Vector(...), VectorAccumulatorParam())
{% endhighlight %}

</div>

</div>

For accumulator updates performed inside <b>actions only</b>, Spark guarantees that each task's update to the accumulator 
will only be applied once, i.e. restarted tasks will not update the value. In transformations, users should be aware 
of that each task's update may be applied more than once if tasks or job stages are re-executed.

Accumulators do not change the lazy evaluation model of Spark. If they are being updated within an operation on an RDD, their value is only updated once that RDD is computed as part of an action. Consequently, accumulator updates are not guaranteed to be executed when made within a lazy transformation like `map()`. The below code fragment demonstrates this property:

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
val acc = sc.accumulator(0)
data.map(x => acc += x; f(x))
// Here, acc is still 0 because no actions have cause the `map` to be computed.
{% endhighlight %}
</div>

<div data-lang="java"  markdown="1">
{% highlight java %}
Accumulator<Integer> accum = sc.accumulator(0);
data.map(x -> accum.add(x); f(x););
// Here, accum is still 0 because no actions have cause the `map` to be computed.
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
accum = sc.accumulator(0)
data.map(lambda x => acc.add(x); f(x))
# Here, acc is still 0 because no actions have cause the `map` to be computed.
{% endhighlight %}
</div>

</div>

# Deploying to a Cluster

The [application submission guide](submitting-applications.html) describes how to submit applications to a cluster.
In short, once you package your application into a JAR (for Java/Scala) or a set of `.py` or `.zip` files (for Python),
the `bin/spark-submit` script lets you submit it to any supported cluster manager.

# Unit Testing

Spark is friendly to unit testing with any popular unit test framework.
Simply create a `SparkContext` in your test with the master URL set to `local`, run your operations,
and then call `SparkContext.stop()` to tear it down.
Make sure you stop the context within a `finally` block or the test framework's `tearDown` method,
as Spark does not support two contexts running concurrently in the same program.

# Migrating from pre-1.0 Versions of Spark

<div class="codetabs">

<div data-lang="scala"  markdown="1">

Spark 1.0 freezes the API of Spark Core for the 1.X series, in that any API available today that is
not marked "experimental" or "developer API" will be supported in future versions.
The only change for Scala users is that the grouping operations, e.g. `groupByKey`, `cogroup` and `join`,
have changed from returning `(Key, Seq[Value])` pairs to `(Key, Iterable[Value])`.

</div>

<div data-lang="java"  markdown="1">

Spark 1.0 freezes the API of Spark Core for the 1.X series, in that any API available today that is
not marked "experimental" or "developer API" will be supported in future versions.
Several changes were made to the Java API:

* The Function classes in `org.apache.spark.api.java.function` became interfaces in 1.0, meaning that old
  code that `extends Function` should `implement Function` instead.
* New variants of the `map` transformations, like `mapToPair` and `mapToDouble`, were added to create RDDs
  of special data types.
* Grouping operations like `groupByKey`, `cogroup` and `join` have changed from returning 
  `(Key, List<Value>)` pairs to `(Key, Iterable<Value>)`.

</div>

<div data-lang="python"  markdown="1">

Spark 1.0 freezes the API of Spark Core for the 1.X series, in that any API available today that is
not marked "experimental" or "developer API" will be supported in future versions.
The only change for Python users is that the grouping operations, e.g. `groupByKey`, `cogroup` and `join`,
have changed from returning (key, list of values) pairs to (key, iterable of values).

</div>

</div>

Migration guides are also available for [Spark Streaming](streaming-programming-guide.html#migration-guide-from-091-or-below-to-1x),
[MLlib](mllib-guide.html#migration-guide) and [GraphX](graphx-programming-guide.html#migrating-from-spark-091).


# Where to Go from Here

You can see some [example Spark programs](http://spark.apache.org/examples.html) on the Spark website.
In addition, Spark includes several samples in the `examples` directory
([Scala]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/scala/org/apache/spark/examples),
 [Java]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/java/org/apache/spark/examples),
 [Python]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/python)).
You can run Java and Scala examples by passing the class name to Spark's `bin/run-example` script; for instance:

    ./bin/run-example SparkPi

For Python examples, use `spark-submit` instead:

    ./bin/spark-submit examples/src/main/python/pi.py

For help on optimizing your programs, the [configuration](configuration.html) and
[tuning](tuning.html) guides provide information on best practices. They are especially important for
making sure that your data is stored in memory in an efficient format.
For help on deploying, the [cluster mode overview](cluster-overview.html) describes the components involved
in distributed operation and supported cluster managers.

Finally, full API documentation is available in
[Scala](api/scala/#org.apache.spark.package), [Java](api/java/) and [Python](api/python/).
