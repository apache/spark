---
layout: global
title: Python Programming Guide
---


The Spark Python API (PySpark) exposes most of the Spark features available in the Scala version to Python.
To learn the basics of Spark, we recommend reading through the
[Scala programming guide](scala-programming-guide.html) first; it should be
easy to follow even if you don't know Scala.
This guide will show how to use the Spark features described there in Python.

# Key Differences in the Python API

There are a few key differences between the Python and Scala APIs:

* Python is dynamically typed, so RDDs can hold objects of different types.
* PySpark does not currently support the following Spark features:
    - Special functions on RDDs of doubles, such as `mean` and `stdev`
    - `lookup`, `sample` and `sort`
    - `persist` at storage levels other than `MEMORY_ONLY`
    - Execution on Windows -- this is slated for a future release

In PySpark, RDDs support the same methods as their Scala counterparts but take Python functions and return Python collection types.
Short functions can be passed to RDD methods using Python's [`lambda`](http://www.diveintopython.net/power_of_introspection/lambda_functions.html) syntax:

{% highlight python %}
logData = sc.textFile(logFile).cache()
errors = logData.filter(lambda line: "ERROR" in line)
{% endhighlight %}

You can also pass functions that are defined using the `def` keyword; this is useful for more complicated functions that cannot be expressed using `lambda`:

{% highlight python %}
def is_error(line):
    return "ERROR" in line
errors = logData.filter(is_error)
{% endhighlight %}

Functions can access objects in enclosing scopes, although modifications to those objects within RDD methods will not be propagated to other tasks:

{% highlight python %}
error_keywords = ["Exception", "Error"]
def is_error(line):
    return any(keyword in line for keyword in error_keywords)
errors = logData.filter(is_error)
{% endhighlight %}

PySpark will automatically ship these functions to workers, along with any objects that they reference.
Instances of classes will be serialized and shipped to workers by PySpark, but classes themselves cannot be automatically distributed to workers.
The [Standalone Use](#standalone-use) section describes how to ship code dependencies to workers.

# Installing and Configuring PySpark

PySpark requires Python 2.6 or higher.
PySpark jobs are executed using a standard cPython interpreter in order to support Python modules that use C extensions.
We have not tested PySpark with Python 3 or with alternative Python interpreters, such as [PyPy](http://pypy.org/) or [Jython](http://www.jython.org/).
By default, PySpark's scripts will run programs using `python`; an alternate Python executable may be specified by setting the `PYSPARK_PYTHON` environment variable in `conf/spark-env.sh`.

All of PySpark's library dependencies, including [Py4J](http://py4j.sourceforge.net/), are bundled with PySpark and automatically imported.

Standalone PySpark jobs should be run using the `pyspark` script, which automatically configures the Java and Python environment using the settings in `conf/spark-env.sh`.
The script automatically adds the `pyspark` package to the `PYTHONPATH`.


# Interactive Use

The `pyspark` script launches a Python interpreter that is configured to run PySpark jobs. To use `pyspark` interactively, first build Spark, then launch it directly from the command line without any options:

{% highlight bash %}
$ sbt/sbt package
$ ./pyspark
{% endhighlight %}

The Python shell can be used explore data interactively and is a simple way to learn the API:

{% highlight python %}
>>> words = sc.textFile("/usr/share/dict/words")
>>> words.filter(lambda w: w.startswith("spar")).take(5)
[u'spar', u'sparable', u'sparada', u'sparadrap', u'sparagrass']
>>> help(pyspark) # Show all pyspark functions
{% endhighlight %}

By default, the `pyspark` shell creates SparkContext that runs jobs locally.
To connect to a non-local cluster, set the `MASTER` environment variable.
For example, to use the `pyspark` shell with a [standalone Spark cluster](spark-standalone.html):

{% highlight bash %}
$ MASTER=spark://IP:PORT ./pyspark
{% endhighlight %}


# Standalone Use

PySpark can also be used from standalone Python scripts by creating a SparkContext in your script and running the script using `pyspark`.
The Quick Start guide includes a [complete example](quick-start.html#a-standalone-job-in-python) of a standalone Python job.

Code dependencies can be deployed by listing them in the `pyFiles` option in the SparkContext constructor:

{% highlight python %}
from pyspark import SparkContext
sc = SparkContext("local", "Job Name", pyFiles=['MyFile.py', 'lib.zip', 'app.egg'])
{% endhighlight %}

Files listed here will be added to the `PYTHONPATH` and shipped to remote worker machines.
Code dependencies can be added to an existing SparkContext using its `addPyFile()` method.

# Where to Go from Here

PySpark includes several sample programs in the [`python/examples` folder](https://github.com/mesos/spark/tree/master/python/examples).
You can run them by passing the files to the `pyspark` script -- for example `./pyspark python/examples/wordcount.py`.
Each program prints usage help when run without arguments.

We currently provide [API documentation](api/pyspark/index.html) for the Python API as Epydoc.
Many of the RDD method descriptions contain [doctests](http://docs.python.org/2/library/doctest.html) that provide additional usage examples.
