---
layout: global
displayTitle: SparkR (R on Spark)
title: SparkR (R on Spark)
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview
SparkR is an R package that provides a light-weight frontend to use Apache Spark from R.
In Spark {{site.SPARK_VERSION}}, SparkR provides a distributed data frame implementation that
supports operations like selection, filtering, aggregation etc. (similar to R data frames,
[dplyr](https://github.com/hadley/dplyr)) but on large datasets. SparkR also supports distributed
machine learning using MLlib.

# SparkR DataFrames

A DataFrame is a distributed collection of data organized into named columns. It is conceptually
equivalent to a table in a relational database or a data frame in R, but with richer
optimizations under the hood. DataFrames can be constructed from a wide array of sources such as:
structured data files, tables in Hive, external databases, or existing local R data frames.

All of the examples on this page use sample data included in R or the Spark distribution and can be run using the `./bin/sparkR` shell.

## Starting Up: SparkContext, SQLContext

<div data-lang="r"  markdown="1">
The entry point into SparkR is the `SparkContext` which connects your R program to a Spark cluster.
You can create a `SparkContext` using `sparkR.init` and pass in options such as the application name
, any spark packages depended on, etc. Further, to work with DataFrames we will need a `SQLContext`,
which can be created from the  SparkContext. If you are working from the `sparkR` shell, the
`SQLContext` and `SparkContext` should already be created for you, and you would not need to call
`sparkR.init`.

<div data-lang="r" markdown="1">
{% highlight r %}
sc <- sparkR.init()
sqlContext <- sparkRSQL.init(sc)
{% endhighlight %}
</div>

## Starting Up from RStudio

You can also start SparkR from RStudio. You can connect your R program to a Spark cluster from
RStudio, R shell, Rscript or other R IDEs. To start, make sure SPARK_HOME is set in environment
(you can check [Sys.getenv](https://stat.ethz.ch/R-manual/R-devel/library/base/html/Sys.getenv.html)),
load the SparkR package, and call `sparkR.init` as below. In addition to calling `sparkR.init`, you
could also specify certain Spark driver properties. Normally these
[Application properties](configuration.html#application-properties) and
[Runtime Environment](configuration.html#runtime-environment) cannot be set programmatically, as the
driver JVM process would have been started, in this case SparkR takes care of this for you. To set
them, pass them as you would other configuration properties in the `sparkEnvir` argument to
`sparkR.init()`.

<div data-lang="r" markdown="1">
{% highlight r %}
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/home/spark")
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sc <- sparkR.init(master = "local[*]", sparkEnvir = list(spark.driver.memory="2g"))
{% endhighlight %}
</div>

The following options can be set in `sparkEnvir` with `sparkR.init` from RStudio:

<table class="table">
  <tr><th>Property Name</th><th>Property group</th><th><code>spark-submit</code> equivalent</th></tr>
  <tr>
    <td><code>spark.driver.memory</code></td>
    <td>Application Properties</td>
    <td><code>--driver-memory</code></td>
  </tr>
  <tr>
    <td><code>spark.driver.extraClassPath</code></td>
    <td>Runtime Environment</td>
    <td><code>--driver-class-path</code></td>
  </tr>
  <tr>
    <td><code>spark.driver.extraJavaOptions</code></td>
    <td>Runtime Environment</td>
    <td><code>--driver-java-options</code></td>
  </tr>
  <tr>
    <td><code>spark.driver.extraLibraryPath</code></td>
    <td>Runtime Environment</td>
    <td><code>--driver-library-path</code></td>
  </tr>
</table>

</div>

## Creating DataFrames
With a `SQLContext`, applications can create `DataFrame`s from a local R data frame, from a [Hive table](sql-programming-guide.html#hive-tables), or from other [data sources](sql-programming-guide.html#data-sources).

### From local data frames
The simplest way to create a data frame is to convert a local R data frame into a SparkR DataFrame. Specifically we can use `createDataFrame` and pass in the local R data frame to create a SparkR DataFrame. As an example, the following creates a `DataFrame` based using the `faithful` dataset from R.

<div data-lang="r"  markdown="1">
{% highlight r %}
df <- createDataFrame(sqlContext, faithful)

# Displays the content of the DataFrame to stdout
head(df)
##  eruptions waiting
##1     3.600      79
##2     1.800      54
##3     3.333      74

{% endhighlight %}
</div>

### From Data Sources

SparkR supports operating on a variety of data sources through the `DataFrame` interface. This section describes the general methods for loading and saving data using Data Sources. You can check the Spark SQL programming guide for more [specific options](sql-programming-guide.html#manually-specifying-options) that are available for the built-in data sources.

The general method for creating DataFrames from data sources is `read.df`. This method takes in the `SQLContext`, the path for the file to load and the type of data source. SparkR supports reading JSON, CSV and Parquet files natively and through [Spark Packages](http://spark-packages.org/) you can find data source connectors for popular file formats like [Avro](http://spark-packages.org/package/databricks/spark-avro). These packages can either be added by
specifying `--packages` with `spark-submit` or `sparkR` commands, or if creating context through `init`
you can specify the packages with the `packages` argument.

<div data-lang="r" markdown="1">
{% highlight r %}
sc <- sparkR.init(sparkPackages="com.databricks:spark-avro_2.11:2.0.1")
sqlContext <- sparkRSQL.init(sc)
{% endhighlight %}
</div>

We can see how to use data sources using an example JSON input file. Note that the file that is used here is _not_ a typical JSON file. Each line in the file must contain a separate, self-contained valid JSON object. As a consequence, a regular multi-line JSON file will most often fail.

<div data-lang="r"  markdown="1">

{% highlight r %}
people <- read.df(sqlContext, "./examples/src/main/resources/people.json", "json")
head(people)
##  age    name
##1  NA Michael
##2  30    Andy
##3  19  Justin

# SparkR automatically infers the schema from the JSON file
printSchema(people)
# root
#  |-- age: long (nullable = true)
#  |-- name: string (nullable = true)

{% endhighlight %}
</div>

The data sources API can also be used to save out DataFrames into multiple file formats. For example we can save the DataFrame from the previous example
to a Parquet file using `write.df` (Until Spark 1.6, the default mode for writes was `append`. It was changed in Spark 1.7 to `error` to match the Scala API)

<div data-lang="r"  markdown="1">
{% highlight r %}
write.df(people, path="people.parquet", source="parquet", mode="overwrite")
{% endhighlight %}
</div>

### From Hive tables

You can also create SparkR DataFrames from Hive tables. To do this we will need to create a HiveContext which can access tables in the Hive MetaStore. Note that Spark should have been built with [Hive support](building-spark.html#building-with-hive-and-jdbc-support) and more details on the difference between SQLContext and HiveContext can be found in the [SQL programming guide](sql-programming-guide.html#starting-point-sqlcontext).

<div data-lang="r" markdown="1">
{% highlight r %}
# sc is an existing SparkContext.
hiveContext <- sparkRHive.init(sc)

sql(hiveContext, "CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
sql(hiveContext, "LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

# Queries can be expressed in HiveQL.
results <- sql(hiveContext, "FROM src SELECT key, value")

# results is now a DataFrame
head(results)
##  key   value
## 1 238 val_238
## 2  86  val_86
## 3 311 val_311

{% endhighlight %}
</div>

## DataFrame Operations

SparkR DataFrames support a number of functions to do structured data processing.
Here we include some basic examples and a complete list can be found in the [API](api/R/index.html) docs:

### Selecting rows, columns

<div data-lang="r"  markdown="1">
{% highlight r %}
# Create the DataFrame
df <- createDataFrame(sqlContext, faithful)

# Get basic information about the DataFrame
df
## SparkDataFrame[eruptions:double, waiting:double]

# Select only the "eruptions" column
head(select(df, df$eruptions))
##  eruptions
##1     3.600
##2     1.800
##3     3.333

# You can also pass in column name as strings
head(select(df, "eruptions"))

# Filter the DataFrame to only retain rows with wait times shorter than 50 mins
head(filter(df, df$waiting < 50))
##  eruptions waiting
##1     1.750      47
##2     1.750      47
##3     1.867      48

{% endhighlight %}

</div>

### Grouping, Aggregation

SparkR data frames support a number of commonly used functions to aggregate data after grouping. For example we can compute a histogram of the `waiting` time in the `faithful` dataset as shown below

<div data-lang="r"  markdown="1">
{% highlight r %}

# We use the `n` operator to count the number of times each waiting time appears
head(summarize(groupBy(df, df$waiting), count = n(df$waiting)))
##  waiting count
##1      70     4
##2      67     1
##3      69     2

# We can also sort the output from the aggregation to get the most common waiting times
waiting_counts <- summarize(groupBy(df, df$waiting), count = n(df$waiting))
head(arrange(waiting_counts, desc(waiting_counts$count)))
##   waiting count
##1      78    15
##2      83    14
##3      81    13

{% endhighlight %}
</div>

### Operating on Columns

SparkR also provides a number of functions that can directly applied to columns for data processing and during aggregation. The example below shows the use of basic arithmetic functions.

<div data-lang="r"  markdown="1">
{% highlight r %}

# Convert waiting time from hours to seconds.
# Note that we can assign this to a new column in the same DataFrame
df$waiting_secs <- df$waiting * 60
head(df)
##  eruptions waiting waiting_secs
##1     3.600      79         4740
##2     1.800      54         3240
##3     3.333      74         4440

{% endhighlight %}
</div>

## Running SQL Queries from SparkR
A SparkR DataFrame can also be registered as a temporary table in Spark SQL and registering a DataFrame as a table allows you to run SQL queries over its data.
The `sql` function enables applications to run SQL queries programmatically and returns the result as a `DataFrame`.

<div data-lang="r"  markdown="1">
{% highlight r %}
# Load a JSON file
people <- read.df(sqlContext, "./examples/src/main/resources/people.json", "json")

# Register this DataFrame as a table.
registerTempTable(people, "people")

# SQL statements can be run by using the sql method
teenagers <- sql(sqlContext, "SELECT name FROM people WHERE age >= 13 AND age <= 19")
head(teenagers)
##    name
##1 Justin

{% endhighlight %}
</div>

# Machine Learning

SparkR supports the following Machine Learning algorithms.

* Generalized Linear Regression Model [spark.glm()](api/R/spark.glm.html)
* Naive Bayes [spark.naiveBayes()](api/R/spark.naiveBayes.html)
* KMeans [spark.kmeans()](api/R/spark.kmeans.html)
* AFT Survival Regression [spark.survreg()](api/R/spark.survreg.html)

[Generalized Linear Regression](api/R/spark.glm.html) can be used to train a model from a specified family. Currently the Gaussian, Binomial, Poisson and Gamma families are supported. We support a subset of the available R formula operators for model fitting, including '~', '.', ':', '+', and '-'.

The [summary()](api/R/summary.html) function gives the summary of a model produced by different algorithms listed above.
It produces the similar result compared with R summary function.

## Model persistence

* [write.ml](api/R/write.ml.html) allows users to save a fitted model in a given input path
* [read.ml](api/R/read.ml.html) allows users to read/load the model which was saved using write.ml in a given path

Model persistence is supported for all Machine Learning algorithms for all families.

The examples below show how to build several models:
* GLM using the Gaussian and Binomial model families
* AFT survival regression model
* Naive Bayes model
* K-Means model

{% include_example r/ml.R %}

# R Function Name Conflicts

When loading and attaching a new package in R, it is possible to have a name [conflict](https://stat.ethz.ch/R-manual/R-devel/library/base/html/library.html), where a
function is masking another function.

The following functions are masked by the SparkR package:

<table class="table">
  <tr><th>Masked function</th><th>How to Access</th></tr>
  <tr>
    <td><code>cov</code> in <code>package:stats</code></td>
    <td><code><pre>stats::cov(x, y = NULL, use = "everything",
           method = c("pearson", "kendall", "spearman"))</pre></code></td>
  </tr>
  <tr>
    <td><code>filter</code> in <code>package:stats</code></td>
    <td><code><pre>stats::filter(x, filter, method = c("convolution", "recursive"),
              sides = 2, circular = FALSE, init)</pre></code></td>
  </tr>
  <tr>
    <td><code>sample</code> in <code>package:base</code></td>
    <td><code>base::sample(x, size, replace = FALSE, prob = NULL)</code></td>
  </tr>
</table>

Since part of SparkR is modeled on the `dplyr` package, certain functions in SparkR share the same names with those in `dplyr`. Depending on the load order of the two packages, some functions from the package loaded first are masked by those in the package loaded after. In such case, prefix such calls with the package name, for instance, `SparkR::cume_dist(x)` or `dplyr::cume_dist(x)`.

You can inspect the search path in R with [`search()`](https://stat.ethz.ch/R-manual/R-devel/library/base/html/search.html)


# Migration Guide

## Upgrading From SparkR 1.5.x to 1.6.x

 - Before Spark 1.6.0, the default mode for writes was `append`. It was changed in Spark 1.6.0 to `error` to match the Scala API.
 - SparkSQL converts `NA` in R to `null` and vice-versa.

## Upgrading From SparkR 1.6.x to 2.0

 - The method `table` has been removed and replaced by `tableToDF`.
 - The class `DataFrame` has been renamed to `SparkDataFrame` to avoid name conflicts.
 - The `sqlContext` parameter is no longer required for these functions: `createDataFrame`, `as.DataFrame`, `read.json`, `jsonFile`, `read.parquet`, `parquetFile`, `read.text`, `sql`, `tables`, `tableNames`, `cacheTable`, `uncacheTable`, `clearCache`, `dropTempTable`, `read.df`, `loadDF`, `createExternalTable`
