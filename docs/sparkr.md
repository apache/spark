---
layout: global
displayTitle: SparkR (R on Spark)
title: SparkR (R on Spark)
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* This will become a table of contents (this text will be scraped).
{:toc}

SparkR is deprecated from Apache Spark 4.0.0 and will be removed in a future version.

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

<table>
  <thead><tr><th>Property Name</th><th>Property group</th><th><code>spark-submit</code> equivalent</th></tr></thead>
  <tr>
    <td><code>spark.master</code></td>
    <td>Application Properties</td>
    <td><code>--master</code></td>
  </tr>
  <tr>
    <td><code>spark.kerberos.keytab</code></td>
    <td>Application Properties</td>
    <td><code>--keytab</code></td>
  </tr>
  <tr>
    <td><code>spark.kerberos.principal</code></td>
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
With a `SparkSession`, applications can create `SparkDataFrame`s from a local R data frame, from a [Hive table](sql-data-sources-hive-tables.html), or from other [data sources](sql-data-sources.html).

### From local data frames
The simplest way to create a data frame is to convert a local R data frame into a SparkDataFrame. Specifically, we can use `as.DataFrame` or `createDataFrame` and pass in the local R data frame to create a SparkDataFrame. As an example, the following creates a `SparkDataFrame` based using the `faithful` dataset from R.

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

SparkR supports operating on a variety of data sources through the `SparkDataFrame` interface. This section describes the general methods for loading and saving data using Data Sources. You can check the Spark SQL programming guide for more [specific options](sql-data-sources-load-save-functions.html#manually-specifying-options) that are available for the built-in data sources.

The general method for creating SparkDataFrames from data sources is `read.df`. This method takes in the path for the file to load and the type of data source, and the currently active SparkSession will be used automatically.
SparkR supports reading JSON, CSV and Parquet files natively, and through packages available from sources like [Third Party Projects](https://spark.apache.org/third-party-projects.html), you can find data source connectors for popular file formats like Avro. These packages can either be added by
specifying `--packages` with `spark-submit` or `sparkR` commands, or if initializing SparkSession with `sparkPackages` parameter when in an interactive R shell or from RStudio.

<div data-lang="r" markdown="1">
{% highlight r %}
sparkR.session(sparkPackages = "org.apache.spark:spark-avro_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION}}")
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

The data sources API natively supports CSV formatted input files. For more information please refer to SparkR [read.df](api/R/reference/read.df.html) API documentation.

<div data-lang="r"  markdown="1">
{% highlight r %}
df <- read.df(csvPath, "csv", header = "true", inferSchema = "true", na.strings = "NA")

{% endhighlight %}
</div>

The data sources API can also be used to save out SparkDataFrames into multiple file formats. For example, we can save the SparkDataFrame from the previous example
to a Parquet file using `write.df`.

<div data-lang="r"  markdown="1">
{% highlight r %}
write.df(people, path = "people.parquet", source = "parquet", mode = "overwrite")
{% endhighlight %}
</div>

### From Hive tables

You can also create SparkDataFrames from Hive tables. To do this we will need to create a SparkSession with Hive support which can access tables in the Hive MetaStore. Note that Spark should have been built with [Hive support](building-spark.html#building-with-hive-and-jdbc-support) and more details can be found in the [SQL programming guide](sql-getting-started.html#starting-point-sparksession). In SparkR, by default it will attempt to create a SparkSession with Hive support enabled (`enableHiveSupport = TRUE`).

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

SparkR data frames support a number of commonly used functions to aggregate data after grouping. For example, we can compute a histogram of the `waiting` time in the `faithful` dataset as shown below

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

In addition to standard aggregations, SparkR supports [OLAP cube](https://en.wikipedia.org/wiki/OLAP_cube) operators `cube`:

<div data-lang="r"  markdown="1">
{% highlight r %}
head(agg(cube(df, "cyl", "disp", "gear"), avg(df$mpg)))
##  cyl  disp gear avg(mpg)
##1  NA 140.8    4     22.8
##2   4  75.7    4     30.4
##3   8 400.0    3     19.2
##4   8 318.0    3     15.5
##5  NA 351.0   NA     15.8
##6  NA 275.8   NA     16.3
{% endhighlight %}
</div>

and `rollup`:

<div data-lang="r"  markdown="1">
{% highlight r %}
head(agg(rollup(df, "cyl", "disp", "gear"), avg(df$mpg)))
##  cyl  disp gear avg(mpg)
##1   4  75.7    4     30.4
##2   8 400.0    3     19.2
##3   8 318.0    3     15.5
##4   4  78.7   NA     32.4
##5   8 304.0    3     15.2
##6   4  79.0   NA     27.3
{% endhighlight %}
</div>

### Operating on Columns

SparkR also provides a number of functions that can be directly applied to columns for data processing and during aggregation. The example below shows the use of basic arithmetic functions.

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

### Eager execution

If eager execution is enabled, the data will be returned to R client immediately when the `SparkDataFrame` is created. By default, eager execution is not enabled and can be enabled by setting the configuration property `spark.sql.repl.eagerEval.enabled` to `true` when the `SparkSession` is started up.

Maximum number of rows and maximum number of characters per column of data to display can be controlled by `spark.sql.repl.eagerEval.maxNumRows` and `spark.sql.repl.eagerEval.truncate` configuration properties, respectively. These properties are only effective when eager execution is enabled. If these properties are not set explicitly, by default, data up to 20 rows and up to 20 characters per column will be showed.

<div data-lang="r" markdown="1">
{% highlight r %}

# Start up spark session with eager execution enabled
sparkR.session(master = "local[*]",
               sparkConfig = list(spark.sql.repl.eagerEval.enabled = "true",
                                  spark.sql.repl.eagerEval.maxNumRows = as.integer(10)))

# Create a grouped and sorted SparkDataFrame
df <- createDataFrame(faithful)
df2 <- arrange(summarize(groupBy(df, df$waiting), count = n(df$waiting)), "waiting")

# Similar to R data.frame, displays the data returned, instead of SparkDataFrame class string
df2

##+-------+-----+
##|waiting|count|
##+-------+-----+
##|   43.0|    1|
##|   45.0|    3|
##|   46.0|    5|
##|   47.0|    4|
##|   48.0|    3|
##|   49.0|    5|
##|   50.0|    5|
##|   51.0|    6|
##|   52.0|    5|
##|   53.0|    7|
##+-------+-----+
##only showing top 10 rows

{% endhighlight %}
</div>

Note that to enable eager execution in `sparkR` shell, add `spark.sql.repl.eagerEval.enabled=true` configuration property to the `--conf` option.

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

* [`spark.logit`](api/R/reference/spark.logit.html): [`Logistic Regression`](ml-classification-regression.html#logistic-regression)
* [`spark.mlp`](api/R/reference/spark.mlp.html): [`Multilayer Perceptron (MLP)`](ml-classification-regression.html#multilayer-perceptron-classifier)
* [`spark.naiveBayes`](api/R/reference/spark.naiveBayes.html): [`Naive Bayes`](ml-classification-regression.html#naive-bayes)
* [`spark.svmLinear`](api/R/reference/spark.svmLinear.html): [`Linear Support Vector Machine`](ml-classification-regression.html#linear-support-vector-machine)
* [`spark.fmClassifier`](api/R/reference/fmClassifier.html): [`Factorization Machines classifier`](ml-classification-regression.html#factorization-machines-classifier)

#### Regression

* [`spark.survreg`](api/R/reference/spark.survreg.html): [`Accelerated Failure Time (AFT) Survival  Model`](ml-classification-regression.html#survival-regression)
* [`spark.glm`](api/R/reference/spark.glm.html) or [`glm`](api/R/reference/glm.html): [`Generalized Linear Model (GLM)`](ml-classification-regression.html#generalized-linear-regression)
* [`spark.isoreg`](api/R/reference/spark.isoreg.html): [`Isotonic Regression`](ml-classification-regression.html#isotonic-regression)
* [`spark.lm`](api/R/reference/spark.lm.html): [`Linear Regression`](ml-classification-regression.html#linear-regression)
* [`spark.fmRegressor`](api/R/reference/spark.fmRegressor.html): [`Factorization Machines regressor`](ml-classification-regression.html#factorization-machines-regressor)

#### Tree

* [`spark.decisionTree`](api/R/reference/spark.decisionTree.html): `Decision Tree for` [`Regression`](ml-classification-regression.html#decision-tree-regression) `and` [`Classification`](ml-classification-regression.html#decision-tree-classifier)
* [`spark.gbt`](api/R/reference/spark.gbt.html): `Gradient Boosted Trees for` [`Regression`](ml-classification-regression.html#gradient-boosted-tree-regression) `and` [`Classification`](ml-classification-regression.html#gradient-boosted-tree-classifier)
* [`spark.randomForest`](api/R/reference/spark.randomForest.html): `Random Forest for` [`Regression`](ml-classification-regression.html#random-forest-regression) `and` [`Classification`](ml-classification-regression.html#random-forest-classifier)

#### Clustering

* [`spark.bisectingKmeans`](api/R/reference/spark.bisectingKmeans.html): [`Bisecting k-means`](ml-clustering.html#bisecting-k-means)
* [`spark.gaussianMixture`](api/R/reference/spark.gaussianMixture.html): [`Gaussian Mixture Model (GMM)`](ml-clustering.html#gaussian-mixture-model-gmm)
* [`spark.kmeans`](api/R/reference/spark.kmeans.html): [`K-Means`](ml-clustering.html#k-means)
* [`spark.lda`](api/R/reference/spark.lda.html): [`Latent Dirichlet Allocation (LDA)`](ml-clustering.html#latent-dirichlet-allocation-lda)
* [`spark.powerIterationClustering (PIC)`](api/R/reference/spark.powerIterationClustering.html): [`Power Iteration Clustering (PIC)`](ml-clustering.html#power-iteration-clustering-pic)

#### Collaborative Filtering

* [`spark.als`](api/R/reference/spark.als.html): [`Alternating Least Squares (ALS)`](ml-collaborative-filtering.html#collaborative-filtering)

#### Frequent Pattern Mining

* [`spark.fpGrowth`](api/R/reference/spark.fpGrowth.html) : [`FP-growth`](ml-frequent-pattern-mining.html#fp-growth)
* [`spark.prefixSpan`](api/R/reference/spark.prefixSpan.html) : [`PrefixSpan`](ml-frequent-pattern-mining.html#prefixspan)

#### Statistics

* [`spark.kstest`](api/R/reference/spark.kstest.html): `Kolmogorov-Smirnov Test`

Under the hood, SparkR uses MLlib to train the model. Please refer to the corresponding section of MLlib user guide for example code.
Users can call `summary` to print a summary of the fitted model, [predict](api/R/reference/predict.html) to make predictions on new data, and [write.ml](api/R/reference/write.ml.html)/[read.ml](api/R/reference/read.ml.html) to save/load fitted models.
SparkR supports a subset of the available R formula operators for model fitting, including ‘~’, ‘.’, ‘:’, ‘+’, and ‘-‘.


## Model persistence

The following example shows how to save/load a MLlib model by SparkR.
{% include_example read_write r/ml/ml.R %}

# Data type mapping between R and Spark
<table>
<thead><tr><th>R</th><th>Spark</th></tr></thead>
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

SparkR supports the Structured Streaming API. Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. For more information see the R API on the [Structured Streaming Programming Guide](./streaming/index.html).

# Apache Arrow in SparkR

Apache Arrow is an in-memory columnar data format that is used in Spark to efficiently transfer data between JVM and R processes. See also PySpark optimization done, [PySpark Usage Guide for Pandas with Apache Arrow](sql-pyspark-pandas-with-arrow.html). This guide targets to explain how to use Arrow optimization in SparkR with some key points.

## Ensure Arrow Installed

Arrow R library is available on CRAN and it can be installed as below.

```bash
Rscript -e 'install.packages("arrow", repos="https://cloud.r-project.org/")'
```
Please refer [the official documentation of Apache Arrow](https://arrow.apache.org/docs/r/) for more details.

Note that you must ensure that Arrow R package is installed and available on all cluster nodes.
The current supported minimum version is 1.0.0; however, this might change between the minor releases since Arrow optimization in SparkR is experimental.

## Enabling for Conversion to/from R DataFrame, `dapply` and `gapply`

Arrow optimization is available when converting a Spark DataFrame to an R DataFrame using the call `collect(spark_df)`,
when creating a Spark DataFrame from an R DataFrame with `createDataFrame(r_df)`, when applying an R native function to each partition
via `dapply(...)` and when applying an R native function to grouped data via `gapply(...)`.
To use Arrow when executing these, users need to set the Spark configuration ‘spark.sql.execution.arrow.sparkr.enabled’
to ‘true’ first. This is disabled by default.

Whether the optimization is enabled or not, SparkR produces the same results. In addition, the conversion
between Spark DataFrame and R DataFrame falls back automatically to non-Arrow optimization implementation
when the optimization fails for any reasons before the actual computation.

<div data-lang="r" markdown="1">
{% highlight r %}
# Start up spark session with Arrow optimization enabled
sparkR.session(master = "local[*]",
               sparkConfig = list(spark.sql.execution.arrow.sparkr.enabled = "true"))

# Converts Spark DataFrame from an R DataFrame
spark_df <- createDataFrame(mtcars)

# Converts Spark DataFrame to an R DataFrame
collect(spark_df)

# Apply an R native function to each partition.
collect(dapply(spark_df, function(rdf) { data.frame(rdf$gear + 1) }, structType("gear double")))

# Apply an R native function to grouped data.
collect(gapply(spark_df,
               "gear",
               function(key, group) {
                 data.frame(gear = key[[1]], disp = mean(group$disp) > group$disp)
               },
               structType("gear double, disp boolean")))
{% endhighlight %}
</div>

Note that even with Arrow, `collect(spark_df)` results in the collection of all records in the DataFrame to
the driver program and should be done on a small subset of the data. In addition, the specified output schema
in `gapply(...)` and `dapply(...)` should be matched to the R DataFrame's returned by the given function.

## Supported SQL Types

Currently, all Spark SQL data types are supported by Arrow-based conversion except `FloatType`, `BinaryType`, `ArrayType`, `StructType` and `MapType`.

# R Function Name Conflicts

When loading and attaching a new package in R, it is possible to have a name [conflict](https://stat.ethz.ch/R-manual/R-devel/library/base/html/library.html), where a
function is masking another function.

The following functions are masked by the SparkR package:

<table>
  <thead><tr><th>Masked function</th><th>How to Access</th></tr></thead>
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

The migration guide is now archived [on this page](sparkr-migration-guide.html).
