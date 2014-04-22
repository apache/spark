---
layout: global
title: Python Programming Guide
---


The Spark Python API (PySpark) exposes the Spark programming model to Python.
To learn the basics of Spark, we recommend reading through the
[Scala programming guide](scala-programming-guide.html) first; it should be
easy to follow even if you don't know Scala.
This guide will show how to use the Spark features described there in Python.


# Key Differences in the Python API

There are a few key differences between the Python and Scala APIs:

* Python is dynamically typed, so RDDs can hold objects of multiple types.
* PySpark does not yet support a few API calls, such as `lookup` and non-text input files, though these will be added in future releases.

In PySpark, RDDs support the same methods as their Scala counterparts but take Python functions and return Python collection types.
Short functions can be passed to RDD methods using Python's [`lambda`](http://www.diveintopython.net/power_of_introspection/lambda_functions.html) syntax:

{% highlight python %}
logData = sc.textFile(logFile).cache()
errors = logData.filter(lambda line: "ERROR" in line)
{% endhighlight %}

You can also pass functions that are defined with the `def` keyword; this is useful for longer functions that can't be expressed using `lambda`:

{% highlight python %}
def is_error(line):
    return "ERROR" in line
errors = logData.filter(is_error)
{% endhighlight %}

Functions can access objects in enclosing scopes, although modifications to those objects within RDD methods will not be propagated back:

{% highlight python %}
error_keywords = ["Exception", "Error"]
def is_error(line):
    return any(keyword in line for keyword in error_keywords)
errors = logData.filter(is_error)
{% endhighlight %}

PySpark will automatically ship these functions to executors, along with any objects that they reference.
Instances of classes will be serialized and shipped to executors by PySpark, but classes themselves cannot be automatically distributed to executors.
The [Standalone Use](#standalone-use) section describes how to ship code dependencies to executors.

In addition, PySpark fully supports interactive use---simply run `./bin/pyspark` to launch an interactive shell.


# Installing and Configuring PySpark

PySpark requires Python 2.6 or higher.
PySpark applications are executed using a standard CPython interpreter in order to support Python modules that use C extensions.
We have not tested PySpark with Python 3 or with alternative Python interpreters, such as [PyPy](http://pypy.org/) or [Jython](http://www.jython.org/).

By default, PySpark requires `python` to be available on the system `PATH` and use it to run programs; an alternate Python executable may be specified by setting the `PYSPARK_PYTHON` environment variable in `conf/spark-env.sh` (or `.cmd` on Windows).

All of PySpark's library dependencies, including [Py4J](http://py4j.sourceforge.net/), are bundled with PySpark and automatically imported.

Standalone PySpark applications should be run using the `bin/pyspark` script, which automatically configures the Java and Python environment using the settings in `conf/spark-env.sh` or `.cmd`.
The script automatically adds the `bin/pyspark` package to the `PYTHONPATH`.


# Interactive Use

The `bin/pyspark` script launches a Python interpreter that is configured to run PySpark applications. To use `pyspark` interactively, first build Spark, then launch it directly from the command line without any options:

{% highlight bash %}
$ sbt/sbt assembly
$ ./bin/pyspark
{% endhighlight %}

The Python shell can be used explore data interactively and is a simple way to learn the API:

{% highlight python %}
>>> words = sc.textFile("/usr/share/dict/words")
>>> words.filter(lambda w: w.startswith("spar")).take(5)
[u'spar', u'sparable', u'sparada', u'sparadrap', u'sparagrass']
>>> help(pyspark) # Show all pyspark functions
{% endhighlight %}

By default, the `bin/pyspark` shell creates SparkContext that runs applications locally on all of
your machine's logical cores.
To connect to a non-local cluster, or to specify a number of cores, set the `MASTER` environment variable.
For example, to use the `bin/pyspark` shell with a [standalone Spark cluster](spark-standalone.html):

{% highlight bash %}
$ MASTER=spark://IP:PORT ./bin/pyspark
{% endhighlight %}

Or, to use exactly four cores on the local machine:

{% highlight bash %}
$ MASTER=local[4] ./bin/pyspark
{% endhighlight %}


## IPython

It is also possible to launch PySpark in [IPython](http://ipython.org), the
enhanced Python interpreter. PySpark works with IPython 1.0.0 and later. To
use IPython, set the `IPYTHON` variable to `1` when running `bin/pyspark`:

{% highlight bash %}
$ IPYTHON=1 ./bin/pyspark
{% endhighlight %}

Alternatively, you can customize the `ipython` command by setting `IPYTHON_OPTS`. For example, to launch
the [IPython Notebook](http://ipython.org/notebook.html) with PyLab graphing support:

{% highlight bash %}
$ IPYTHON_OPTS="notebook --pylab inline" ./bin/pyspark
{% endhighlight %}

IPython also works on a cluster or on multiple cores if you set the `MASTER` environment variable.


# Standalone Programs

PySpark can also be used from standalone Python scripts by creating a SparkContext in your script and running the script using `bin/pyspark`.
The Quick Start guide includes a [complete example](quick-start.html#a-standalone-app-in-python) of a standalone Python application.

Code dependencies can be deployed by listing them in the `pyFiles` option in the SparkContext constructor:

{% highlight python %}
from pyspark import SparkContext
sc = SparkContext("local", "App Name", pyFiles=['MyFile.py', 'lib.zip', 'app.egg'])
{% endhighlight %}

Files listed here will be added to the `PYTHONPATH` and shipped to remote worker machines.
Code dependencies can be added to an existing SparkContext using its `addPyFile()` method.

You can set [configuration properties](configuration.html#spark-properties) by passing a
[SparkConf](api/pyspark/pyspark.conf.SparkConf-class.html) object to SparkContext:

{% highlight python %}
from pyspark import SparkConf, SparkContext
conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)
{% endhighlight %}

# SequenceFile and Hadoop InputFormats

In addition to reading text files, PySpark supports reading Hadoop SequenceFile and arbitrary InputFormats.

## Writable Support

PySpark SequenceFile support loads an RDD within Java, and pickles the resulting Java objects using
[Pyrolite](https://github.com/irmen/Pyrolite/). The following Writables are automatically converted:


<table class="table">
<tr><th>Writable Type</th><th>Scala Type</th><th>Python Type</th></tr>
<tr><td>Text</td><td>String</td><td>unicode str</td></tr>
<tr><td>IntWritable</td><td>Int</td><td>int</td></tr>
<tr><td>FloatWritable</td><td>Float</td><td>float</td></tr>
<tr><td>DoubleWritable</td><td>Double</td><td>float</td></tr>
<tr><td>BooleanWritable</td><td>Boolean</td><td>bool</td></tr>
<tr><td>BytesWritable</td><td>Array[Byte]</td><td>bytearray</td></tr>
<tr><td>NullWritable</td><td>null</td><td>None</td></tr>
<tr><td>ArrayWritable</td><td>Array[T]</td><td>list of primitives, or tuple of objects</td></tr>
<tr><td>MapWritable</td><td>java.util.Map[K, V]</td><td>dict</td></tr>
<tr><td>Custom Class</td><td>Custom Class conforming to Java Bean conventions</td>
    <td>dict of public properties (via JavaBean getters and setters) + __class__ for the class type</td></tr>
</table>

## Loading SequenceFiles

Similarly to text files, SequenceFiles can be loaded by specifying the path. The key and value
classes can be specified, but for standard Writables it should work without requiring this.

{% highlight python %}
>>> rdd = sc.sequenceFile("path/to/sequencefile/of/doubles")
>>> rdd.collect()         # this example has DoubleWritable keys and Text values
[(1.0, u'aa'),
 (2.0, u'bb'),
 (2.0, u'aa'),
 (3.0, u'cc'),
 (2.0, u'bb'),
 (1.0, u'aa')]
>>> help(sc.sequenceFile) # Show sequencefile documentation
{% endhighlight %}

## Loading Arbitrary Hadoop InputFormats

PySpark can also read any Hadoop InputFormat, for both 'new' and 'old' Hadoop APIs. If required,
a Hadoop configuration can be passed in as a Python dict. Here is an example using the
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
>>> help(sc.newAPIHadoopRDD) # Show help for new API Hadoop RDD
{% endhighlight %}

Note that, if the InputFormat simply depends on a Hadoop configuration and/or input path, and
the key and value classes can easily be converted according to the above table,
then this approach should work well for such cases.

If you have custom serialized binary data (like pulling data from Cassandra / HBase) or custom
classes that don't conform to the JavaBean requirements, then you will probably have to first
transform that data on the Scala/Java side to something which can be handled by Pyrolite's pickler.

Future support for 'wrapper' functions for keys/values that allows this to be written in Java/Scala,
and called from Python, as well as support for writing data out as SequenceFile format
and other OutputFormats, is forthcoming.

# API Docs

[API documentation](api/pyspark/index.html) for PySpark is available as Epydoc.
Many of the methods also contain [doctests](http://docs.python.org/2/library/doctest.html) that provide additional usage examples.

# Libraries

[MLlib](mllib-guide.html) is also available in PySpark. To use it, you'll need
[NumPy](http://www.numpy.org) version 1.4 or newer. The [MLlib guide](mllib-guide.html) contains
some example applications.

# Where to Go from Here

PySpark also includes several sample programs in the [`python/examples` folder](https://github.com/apache/spark/tree/master/python/examples).
You can run them by passing the files to `pyspark`; e.g.:

    ./bin/pyspark python/examples/wordcount.py

Each program prints usage help when run without arguments.
