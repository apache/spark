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
The [Standalone Use](#standalone-programs) section describes how to ship code dependencies to executors.

In addition, PySpark fully supports interactive use---simply run `./bin/pyspark` to launch an interactive shell.


# Installing and Configuring PySpark

PySpark requires Python 2.6 or higher.
PySpark applications are executed using a standard CPython interpreter in order to support Python modules that use C extensions.
We have not tested PySpark with Python 3 or with alternative Python interpreters, such as [PyPy](http://pypy.org/) or [Jython](http://www.jython.org/).

By default, PySpark requires `python` to be available on the system `PATH` and use it to run programs; an alternate Python executable may be specified by setting the `PYSPARK_PYTHON` environment variable in `conf/spark-env.sh` (or `.cmd` on Windows).

All of PySpark's library dependencies, including [Py4J](http://py4j.sourceforge.net/), are bundled with PySpark and automatically imported.

# Interactive Use

The `bin/pyspark` script launches a Python interpreter that is configured to run PySpark applications. To use `pyspark` interactively, first build Spark, then launch it directly from the command line:

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
your machine's logical cores. To connect to a non-local cluster, or to specify a number of cores,
set the `--master` flag. For example, to use the `bin/pyspark` shell with a
[standalone Spark cluster](spark-standalone.html):

{% highlight bash %}
$ ./bin/pyspark --master spark://1.2.3.4:7077
{% endhighlight %}

Or, to use exactly four cores on the local machine:

{% highlight bash %}
$ ./bin/pyspark --master local[4]
{% endhighlight %}

Under the hood `bin/pyspark` is a wrapper around the
[Spark submit script](cluster-overview.html#launching-applications-with-spark-submit), so these
two scripts share the same list of options. For a complete list of options, run `bin/pyspark` with
the `--help` option.

## IPython

It is also possible to launch the PySpark shell in [IPython](http://ipython.org), the
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

IPython also works on a cluster or on multiple cores if you set the `--master` flag.


# Standalone Programs

PySpark can also be used from standalone Python scripts by creating a SparkContext in your script
and running the script using `bin/spark-submit`. The Quick Start guide includes a
[complete example](quick-start.html#standalone-applications) of a standalone Python application.

Code dependencies can be deployed by passing .zip or .egg files in the `--py-files` option of `spark-submit`:

{% highlight bash %}
./bin/spark-submit --py-files lib1.zip,lib2.zip my_script.py
{% endhighlight %}

Files listed here will be added to the `PYTHONPATH` and shipped to remote worker machines.
Code dependencies can also be added to an existing SparkContext at runtime using its `addPyFile()` method.

You can set [configuration properties](configuration.html#spark-properties) by passing a
[SparkConf](api/python/pyspark.conf.SparkConf-class.html) object to SparkContext:

{% highlight python %}
from pyspark import SparkConf, SparkContext
conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)
{% endhighlight %}

`spark-submit` supports launching Python applications on standalone, Mesos or YARN clusters, through
its `--master` argument. However, it currently requires the Python driver program to run on the local
machine, not the cluster (i.e. the `--deploy-mode` parameter cannot be `cluster`).


# API Docs

[API documentation](api/python/index.html) for PySpark is available as Epydoc.
Many of the methods also contain [doctests](http://docs.python.org/2/library/doctest.html) that provide additional usage examples.

# Libraries

[MLlib](mllib-guide.html) is also available in PySpark. To use it, you'll need
[NumPy](http://www.numpy.org) version 1.4 or newer. The [MLlib guide](mllib-guide.html) contains
some example applications.

# Where to Go from Here

PySpark also includes several sample programs in the [`examples/src/main/python` folder](https://github.com/apache/spark/tree/master/examples/src/main/python).
You can run them by passing the files to `pyspark`; e.g.:

    ./bin/spark-submit examples/src/main/python/wordcount.py README.md

Each program prints usage help when run without the sufficient arguments.
