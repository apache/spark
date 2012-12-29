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
    - Accumulators
    - Special functions on RRDs of doubles, such as `mean` and `stdev`
    - Approximate jobs / functions, such as `countApprox` and `sumApprox`.
    - `lookup`
    - `mapPartitionsWithSplit`
    - `persist` at storage levels other than `MEMORY_ONLY`
    - `sample`
    - `sort`


# Installing and Configuring PySpark

PySpark requires Python 2.6 or higher.
PySpark jobs are executed using a standard cPython interpreter in order to support Python modules that use C extensions.
We have not tested PySpark with Python 3 or with alternative Python interpreters, such as [PyPy](http://pypy.org/) or [Jython](http://www.jython.org/).
By default, PySpark's scripts will run programs using `python`; an alternate Python executable may be specified by setting the `PYSPARK_PYTHON` environment variable in `conf/spark-env.sh`.

All of PySpark's library dependencies, including [Py4J](http://py4j.sourceforge.net/), are bundled with PySpark and automatically imported.

Standalone PySpark jobs should be run using the `run-pyspark` script, which automatically configures the Java and Python environmnt using the settings in `conf/spark-env.sh`.
The script automatically adds the `pyspark` package to the `PYTHONPATH`.


# Interactive Use

PySpark's `pyspark-shell` script provides a simple way to learn the API:

{% highlight python %}
>>> words = sc.textFile("/usr/share/dict/words")
>>> words.filter(lambda w: w.startswith("spar")).take(5)
[u'spar', u'sparable', u'sparada', u'sparadrap', u'sparagrass']
{% endhighlight %}

# Standalone Use

PySpark can also be used from standalone Python scripts by creating a SparkContext in the script and running the script using the `run-pyspark` script in the `pyspark` directory.
The Quick Start guide includes a [complete example](quick-start.html#a-standalone-job-in-python) of a standalone Python job.

Code dependencies can be deployed by listing them in the `pyFiles` option in the SparkContext constructor:

{% highlight python %}
from pyspark import SparkContext
sc = SparkContext("local", "Job Name", pyFiles=['MyFile.py', 'lib.zip', 'app.egg'])
{% endhighlight %}

Files listed here will be added to the `PYTHONPATH` and shipped to remote worker machines.
Code dependencies can be added to an existing SparkContext using its `addPyFile()` method.

# Where to Go from Here

PySpark includes several sample programs using the Python API in `pyspark/examples`.
You can run them by passing the files to the `pyspark-run` script included in PySpark -- for example `./pyspark-run examples/wordcount.py`.
Each example program prints usage help when run without any arguments.

We currently provide [API documentation](api/pyspark/index.html) for the Python API as Epydoc.
Many of the RDD method descriptions contain [doctests](http://docs.python.org/2/library/doctest.html) that provide additional usage examples.
