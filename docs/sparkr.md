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

# SparkDataFrame

A SparkDataFrame is a distributed collection of data organized into named columns. It is conceptually
equivalent to a table in a relational database or a data frame in R, but with richer
optimizations under the hood. SparkDataFrames can be constructed from a wide array of sources such as:
structured data files, tables in Hive, external databases, or existing local R data frames.

All of the examples on this page use sample data included in R or the Spark distribution and can be run using the `./bin/sparkR` shell.

## Starting Up: SparkSession

<div data-lang="r"  markdown="1">
The entry point into SparkR is the `SparkSession` which connects your R program to a Spark cluster.
You can create a `SparkSession` using `sparkR.session` and pass in options such as the application name, any spark packages depended on, etc. Further, you can also work with SparkDataFrames via `SparkSession`. If you are working from the `sparkR` shell, the `SparkSession` should already be created for you, and you would not need to call `sparkR.session`.

<div data-lang="r" markdown="1">
{% highlight r %}
sparkR.session()
{% endhighlight %}
</div>

## Starting Up from RStudio

You can also start SparkR from RStudio. You can connect your R program to a Spark cluster from
RStudio, R shell, Rscript or other R IDEs. To start, make sure SPARK_HOME is set in environment
(you can check [Sys.getenv](https://stat.ethz.ch/R-manual/R-devel/library/base/html/Sys.getenv.html)),
load the SparkR package, and call `sparkR.session` as below. It will check for the Spark installation, and, if not found, it will be downloaded and cached automatically. Alternatively, you can also run `install.spark` manually.

In addition to calling `sparkR.session`,
 you could also specify certain Spark driver properties. Normally these
[Application properties](configuration.html#application-properties) and
[Runtime Environment](configuration.html#runtime-environment) cannot be set programmatically, as the
driver JVM process would have been started, in this case SparkR takes care of this for you. To set
them, pass them as you would other configuration properties in the `sparkConfig` argument to
`sparkR.session()`.

<div data-lang="r" markdown="1">
{% highlight r %}
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/home/spark")
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))
{% endhighlight %}
</div>

The following Spark driver properties can be set in `sparkConfig` with `sparkR.session` from RStudio:

<table class="table">
  <tr><th>Property Name</th><th>Property group</th><th><code>spark-submit</code> equivalent</th></tr>
  <tr>
    <td><code>spark.master</code></td>
    <td>Application Properties</td>
    <td><code>--master</code></td>
  </tr>
  <tr>
    <td><code>spark.yarn.keytab</code></td>
    <td>Application Properties</td>
    <td><code>--keytab</code></td>
  </tr>
  <tr>
    <td><code>spark.yarn.principal</code></td>
    <td>Application Properties</td>
    <td><code>--principal</code></td>
  </tr>
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

## Creating SparkDataFrames
With a `SparkSession`, applications can create `SparkDataFrame`s from a local R data frame, from a [Hive table](sql-programming-guide.html#hive-tables), or from other [data sources](sql-programming-guide.html#data-sources).

### From local data frames
The simplest way to create a data frame is to convert a local R data frame into a SparkDataFrame. Specifically we can use `as.DataFrame` or `createDataFrame` and pass in the local R data frame to create a SparkDataFrame. As an example, the following creates a `SparkDataFrame` based using the `faithful` dataset from R.

<div data-lang="r"  markdown="1">
{% highlight r %}
df <- as.DataFrame(faithful)

# Displays the first part of the SparkDataFrame
head(df)
##  eruptions waiting
##1     3.600      79
##2     1.800      54
##3     3.333      74

{% endhighlight %}
</div>

### From Data Sources

SparkR supports operating on a variety of data sources through the `SparkDataFrame` interface. This section describes the general methods for loading and saving data using Data Sources. You can check the Spark SQL programming guide for more [specific options](sql-programming-guide.html#manually-specifying-options) that are available for the built-in data sources.

The general method for creating SparkDataFrames from data sources is `read.df`. This method takes in the path for the file to load and the type of data source, and the currently active SparkSession will be used automatically.
SparkR supports reading JSON, CSV and Parquet files natively, and through packages available from sources like [Third Party Projects](http://spark.apache.org/third-party-projects.html), you can find data source connectors for popular file formats like Avro. These packages can either be added by
specifying `--packages` with `spark-submit` or `sparkR` commands, or if initializing SparkSession with `sparkPackages` parameter when in an interactive R shell or from RStudio.

<div data-lang="r" markdown="1">
{% highlight r %}
sparkR.session(sparkPackages = "com.databricks:spark-avro_2.11:3.0.0")
{% endhighlight %}
</div>

We can see how to use data sources using an example JSON input file. Note that the file that is used here is _not_ a typical JSON file. Each line in the file must contain a separate, self-contained valid JSON object. For more information, please see [JSON Lines text format, also called newline-delimited JSON](http://jsonlines.org/). As a consequence, a regular multi-line JSON file will most often fail.

<div data-lang="r"  markdown="1">
{% highlight r %}
people <- read.df("./examples/src/main/resources/people.json", "json")
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

# Similarly, multiple files can be read with read.json
people <- read.json(c("./examples/src/main/resources/people.json", "./examples/src/main/resources/people2.json"))

{% endhighlight %}
</div>

The data sources API natively supports CSV formatted input files. For more information please refer to SparkR [read.df](api/R/read.df.html) API documentation.

<div data-lang="r"  markdown="1">
{% highlight r %}
df <- read.df(csvPath, "csv", header = "true", inferSchema = "true", na.strings = "NA")

{% endhighlight %}
</div>

The data sources API can also be used to save out SparkDataFrames into multiple file formats. For example we can save the SparkDataFrame from the previous example
to a Parquet file using `write.df`.

<div data-lang="r"  markdown="1">
{% highlight r %}
write.df(people, path = "people.parquet", source = "parquet", mode = "overwrite")
{% endhighlight %}
</div>

### From Hive tables

You can also create SparkDataFrames from Hive tables. To do this we will need to create a SparkSession with Hive support which can access tables in the Hive MetaStore. Note that Spark should have been built with [Hive support](building-spark.html#building-with-hive-and-jdbc-support) and more details can be found in the [SQL programming guide](sql-programming-guide.html#starting-point-sparksession). In SparkR, by default it will attempt to create a SparkSession with Hive support enabled (`enableHiveSupport = TRUE`).

<div data-lang="r" markdown="1">
{% highlight r %}
sparkR.session()

sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

# Queries can be expressed in HiveQL.
results <- sql("FROM src SELECT key, value")

# results is now a SparkDataFrame
head(results)
##  key   value
## 1 238 val_238
## 2  86  val_86
## 3 311 val_311

{% endhighlight %}
</div>

## SparkDataFrame Operations

SparkDataFrames support a number of functions to do structured data processing.
Here we include some basic examples and a complete list can be found in the [API](api/R/index.html) docs:

### Selecting rows, columns

<div data-lang="r"  markdown="1">
{% highlight r %}
# Create the SparkDataFrame
df <- as.DataFrame(faithful)

# Get basic information about the SparkDataFrame
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

# Filter the SparkDataFrame to only retain rows with wait times shorter than 50 mins
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
# Note that we can assign this to a new column in the same SparkDataFrame
df$waiting_secs <- df$waiting * 60
head(df)
##  eruptions waiting waiting_secs
##1     3.600      79         4740
##2     1.800      54         3240
##3     3.333      74         4440

{% endhighlight %}
</div>

### Applying User-Defined Function
In SparkR, we support several kinds of User-Defined Functions:

#### Run a given function on a large dataset using `dapply` or `dapplyCollect`

##### dapply
Apply a function to each partition of a `SparkDataFrame`. The function to be applied to each partition of the `SparkDataFrame`
and should have only one parameter, to which a `data.frame` corresponds to each partition will be passed. The output of function should be a `data.frame`. Schema specifies the row format of the resulting a `SparkDataFrame`. It must match to [data types](#data-type-mapping-between-r-and-spark) of returned value.

<div data-lang="r"  markdown="1">
{% highlight r %}

# Convert waiting time from hours to seconds.
# Note that we can apply UDF to DataFrame.
schema <- structType(structField("eruptions", "double"), structField("waiting", "double"),
                     structField("waiting_secs", "double"))
df1 <- dapply(df, function(x) { x <- cbind(x, x$waiting * 60) }, schema)
head(collect(df1))
##  eruptions waiting waiting_secs
##1     3.600      79         4740
##2     1.800      54         3240
##3     3.333      74         4440
##4     2.283      62         3720
##5     4.533      85         5100
##6     2.883      55         3300
{% endhighlight %}
</div>

##### dapplyCollect
Like `dapply`, apply a function to each partition of a `SparkDataFrame` and collect the result back. The output of function
should be a `data.frame`. But, Schema is not required to be passed. Note that `dapplyCollect` can fail if the output of UDF run on all the partition cannot be pulled to the driver and fit in driver memory.

<div data-lang="r"  markdown="1">
{% highlight r %}

# Convert waiting time from hours to seconds.
# Note that we can apply UDF to DataFrame and return a R's data.frame
ldf <- dapplyCollect(
         df,
         function(x) {
           x <- cbind(x, "waiting_secs" = x$waiting * 60)
         })
head(ldf, 3)
##  eruptions waiting waiting_secs
##1     3.600      79         4740
##2     1.800      54         3240
##3     3.333      74         4440

{% endhighlight %}
</div>

#### Run a given function on a large dataset grouping by input column(s) and using `gapply` or `gapplyCollect`

##### gapply
Apply a function to each group of a `SparkDataFrame`. The function is to be applied to each group of the `SparkDataFrame` and should have only two parameters: grouping key and R `data.frame` corresponding to
that key. The groups are chosen from `SparkDataFrame`s column(s).
The output of function should be a `data.frame`. Schema specifies the row format of the resulting
`SparkDataFrame`. It must represent R function's output schema on the basis of Spark [data types](#data-type-mapping-between-r-and-spark). The column names of the returned `data.frame` are set by user.

<div data-lang="r"  markdown="1">
{% highlight r %}

# Determine six waiting times with the largest eruption time in minutes.
schema <- structType(structField("waiting", "double"), structField("max_eruption", "double"))
result <- gapply(
    df,
    "waiting",
    function(key, x) {
        y <- data.frame(key, max(x$eruptions))
    },
    schema)
head(collect(arrange(result, "max_eruption", decreasing = TRUE)))

##    waiting   max_eruption
##1      64       5.100
##2      69       5.067
##3      71       5.033
##4      87       5.000
##5      63       4.933
##6      89       4.900
{% endhighlight %}
</div>

##### gapplyCollect
Like `gapply`, applies a function to each partition of a `SparkDataFrame` and collect the result back to R data.frame. The output of the function should be a `data.frame`. But, the schema is not required to be passed. Note that `gapplyCollect` can fail if the output of UDF run on all the partition cannot be pulled to the driver and fit in driver memory.

<div data-lang="r"  markdown="1">
{% highlight r %}

# Determine six waiting times with the largest eruption time in minutes.
result <- gapplyCollect(
    df,
    "waiting",
    function(key, x) {
        y <- data.frame(key, max(x$eruptions))
        colnames(y) <- c("waiting", "max_eruption")
        y
    })
head(result[order(result$max_eruption, decreasing = TRUE), ])

##    waiting   max_eruption
##1      64       5.100
##2      69       5.067
##3      71       5.033
##4      87       5.000
##5      63       4.933
##6      89       4.900

{% endhighlight %}
</div>

#### Run local R functions distributed using `spark.lapply`

##### spark.lapply
Similar to `lapply` in native R, `spark.lapply` runs a function over a list of elements and distributes the computations with Spark.
Applies a function in a manner that is similar to `doParallel` or `lapply` to elements of a list. The results of all the computations
should fit in a single machine. If that is not the case they can do something like `df <- createDataFrame(list)` and then use
`dapply`

<div data-lang="r"  markdown="1">
{% highlight r %}
# Perform distributed training of multiple models with spark.lapply. Here, we pass
# a read-only list of arguments which specifies family the generalized linear model should be.
families <- c("gaussian", "poisson")
train <- function(family) {
  model <- glm(Sepal.Length ~ Sepal.Width + Species, iris, family = family)
  summary(model)
}
# Return a list of model's summaries
model.summaries <- spark.lapply(families, train)

# Print the summary of each model
print(model.summaries)

{% endhighlight %}
</div>

## Running SQL Queries from SparkR
A SparkDataFrame can also be registered as a temporary view in Spark SQL and that allows you to run SQL queries over its data.
The `sql` function enables applications to run SQL queries programmatically and returns the result as a `SparkDataFrame`.

<div data-lang="r"  markdown="1">
{% highlight r %}
# Load a JSON file
people <- read.df("./examples/src/main/resources/people.json", "json")

# Register this SparkDataFrame as a temporary view.
createOrReplaceTempView(people, "people")

# SQL statements can be run by using the sql method
teenagers <- sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
head(teenagers)
##    name
##1 Justin

{% endhighlight %}
</div>

# Machine Learning

## Algorithms

SparkR supports the following machine learning algorithms currently:

#### Classification

* [`spark.logit`](api/R/spark.logit.html): [`Logistic Regression`](ml-classification-regression.html#logistic-regression)
* [`spark.mlp`](api/R/spark.mlp.html): [`Multilayer Perceptron (MLP)`](ml-classification-regression.html#multilayer-perceptron-classifier)
* [`spark.naiveBayes`](api/R/spark.naiveBayes.html): [`Naive Bayes`](ml-classification-regression.html#naive-bayes)
* [`spark.svmLinear`](api/R/spark.svmLinear.html): [`Linear Support Vector Machine`](ml-classification-regression.html#linear-support-vector-machine)

#### Regression

* [`spark.survreg`](api/R/spark.survreg.html): [`Accelerated Failure Time (AFT) Survival  Model`](ml-classification-regression.html#survival-regression)
* [`spark.glm`](api/R/spark.glm.html) or [`glm`](api/R/glm.html): [`Generalized Linear Model (GLM)`](ml-classification-regression.html#generalized-linear-regression)
* [`spark.isoreg`](api/R/spark.isoreg.html): [`Isotonic Regression`](ml-classification-regression.html#isotonic-regression)

#### Tree

* [`spark.gbt`](api/R/spark.gbt.html): `Gradient Boosted Trees for` [`Regression`](ml-classification-regression.html#gradient-boosted-tree-regression) `and` [`Classification`](ml-classification-regression.html#gradient-boosted-tree-classifier)
* [`spark.randomForest`](api/R/spark.randomForest.html): `Random Forest for` [`Regression`](ml-classification-regression.html#random-forest-regression) `and` [`Classification`](ml-classification-regression.html#random-forest-classifier)

#### Clustering

* [`spark.bisectingKmeans`](api/R/spark.bisectingKmeans.html): [`Bisecting k-means`](ml-clustering.html#bisecting-k-means)
* [`spark.gaussianMixture`](api/R/spark.gaussianMixture.html): [`Gaussian Mixture Model (GMM)`](ml-clustering.html#gaussian-mixture-model-gmm)
* [`spark.kmeans`](api/R/spark.kmeans.html): [`K-Means`](ml-clustering.html#k-means)
* [`spark.lda`](api/R/spark.lda.html): [`Latent Dirichlet Allocation (LDA)`](ml-clustering.html#latent-dirichlet-allocation-lda)

#### Collaborative Filtering

* [`spark.als`](api/R/spark.als.html): [`Alternating Least Squares (ALS)`](ml-collaborative-filtering.html#collaborative-filtering)

#### Frequent Pattern Mining

* [`spark.fpGrowth`](api/R/spark.fpGrowth.html) : [`FP-growth`](ml-frequent-pattern-mining.html#fp-growth)

#### Statistics

* [`spark.kstest`](api/R/spark.kstest.html): `Kolmogorov-Smirnov Test`

Under the hood, SparkR uses MLlib to train the model. Please refer to the corresponding section of MLlib user guide for example code.
Users can call `summary` to print a summary of the fitted model, [predict](api/R/predict.html) to make predictions on new data, and [write.ml](api/R/write.ml.html)/[read.ml](api/R/read.ml.html) to save/load fitted models.
SparkR supports a subset of the available R formula operators for model fitting, including ‘~’, ‘.’, ‘:’, ‘+’, and ‘-‘.


## Model persistence

The following example shows how to save/load a MLlib model by SparkR.
{% include_example read_write r/ml/ml.R %}

# Data type mapping between R and Spark
<table class="table">
<tr><th>R</th><th>Spark</th></tr>
<tr>
  <td>byte</td>
  <td>byte</td>
</tr>
<tr>
  <td>integer</td>
  <td>integer</td>
</tr>
<tr>
  <td>float</td>
  <td>float</td>
</tr>
<tr>
  <td>double</td>
  <td>double</td>
</tr>
<tr>
  <td>numeric</td>
  <td>double</td>
</tr>
<tr>
  <td>character</td>
  <td>string</td>
</tr>
<tr>
  <td>string</td>
  <td>string</td>
</tr>
<tr>
  <td>binary</td>
  <td>binary</td>
</tr>
<tr>
  <td>raw</td>
  <td>binary</td>
</tr>
<tr>
  <td>logical</td>
  <td>boolean</td>
</tr>
<tr>
  <td><a href="https://stat.ethz.ch/R-manual/R-devel/library/base/html/DateTimeClasses.html">POSIXct</a></td>
  <td>timestamp</td>
</tr>
<tr>
  <td><a href="https://stat.ethz.ch/R-manual/R-devel/library/base/html/DateTimeClasses.html">POSIXlt</a></td>
  <td>timestamp</td>
</tr>
<tr>
  <td><a href="https://stat.ethz.ch/R-manual/R-devel/library/base/html/Dates.html">Date</a></td>
  <td>date</td>
</tr>
<tr>
  <td>array</td>
  <td>array</td>
</tr>
<tr>
  <td>list</td>
  <td>array</td>
</tr>
<tr>
  <td>env</td>
  <td>map</td>
</tr>
</table>

# Structured Streaming

SparkR supports the Structured Streaming API (experimental). Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. For more information see the R API on the [Structured Streaming Programming Guide](structured-streaming-programming-guide.html)

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
 - Spark's `SQLContext` and `HiveContext` have been deprecated to be replaced by `SparkSession`. Instead of `sparkR.init()`, call `sparkR.session()` in its place to instantiate the SparkSession. Once that is done, that currently active SparkSession will be used for SparkDataFrame operations.
 - The parameter `sparkExecutorEnv` is not supported by `sparkR.session`. To set environment for the executors, set Spark config properties with the prefix "spark.executorEnv.VAR_NAME", for example, "spark.executorEnv.PATH"
 - The `sqlContext` parameter is no longer required for these functions: `createDataFrame`, `as.DataFrame`, `read.json`, `jsonFile`, `read.parquet`, `parquetFile`, `read.text`, `sql`, `tables`, `tableNames`, `cacheTable`, `uncacheTable`, `clearCache`, `dropTempTable`, `read.df`, `loadDF`, `createExternalTable`.
 - The method `registerTempTable` has been deprecated to be replaced by `createOrReplaceTempView`.
 - The method `dropTempTable` has been deprecated to be replaced by `dropTempView`.
 - The `sc` SparkContext parameter is no longer required for these functions: `setJobGroup`, `clearJobGroup`, `cancelJobGroup`

## Upgrading to SparkR 2.1.0

 - `join` no longer performs Cartesian Product by default, use `crossJoin` instead.

## Upgrading to SparkR 2.2.0

 - A `numPartitions` parameter has been added to `createDataFrame` and `as.DataFrame`. When splitting the data, the partition position calculation has been made to match the one in Scala.
 - The method `createExternalTable` has been deprecated to be replaced by `createTable`. Either methods can be called to create external or managed table. Additional catalog methods have also been added.
 - By default, derby.log is now saved to `tempdir()`. This will be created when instantiating the SparkSession with `enableHiveSupport` set to `TRUE`.
 - `spark.lda` was not setting the optimizer correctly. It has been corrected.
 - Several model summary outputs are updated to have `coefficients` as `matrix`. This includes `spark.logit`, `spark.kmeans`, `spark.glm`. Model summary outputs for `spark.gaussianMixture` have added log-likelihood as `loglik`.
