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
which can be created from the  SparkContext. If you are working from the SparkR shell, the
`SQLContext` and `SparkContext` should already be created for you.

{% highlight r %}
sc <- sparkR.init()
sqlContext <- sparkRSQL.init(sc)
{% endhighlight %}

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

The general method for creating DataFrames from data sources is `read.df`. This method takes in the `SQLContext`, the path for the file to load and the type of data source. SparkR supports reading JSON and Parquet files natively and through [Spark Packages](http://spark-packages.org/) you can find data source connectors for popular file formats like [CSV](http://spark-packages.org/package/databricks/spark-csv) and [Avro](http://spark-packages.org/package/databricks/spark-avro). These packages can either be added by
specifying `--packages` with `spark-submit` or `sparkR` commands, or if creating context through `init`
you can specify the packages with the `packages` argument.

<div data-lang="r" markdown="1">
{% highlight r %}
sc <- sparkR.init(sparkPackages="com.databricks:spark-csv_2.11:1.0.3")
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
#  |-- age: integer (nullable = true)
#  |-- name: string (nullable = true)

{% endhighlight %}
</div>

The data sources API can also be used to save out DataFrames into multiple file formats. For example we can save the DataFrame from the previous example
to a Parquet file using `write.df` 

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
## DataFrame[eruptions:double, waiting:double]

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
##1      81    13
##2      60     6
##3      68     1

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

SparkR allows the fitting of generalized linear models over DataFrames using the [glm()](api/R/glm.html) function. Under the hood, SparkR uses MLlib to train a model of the specified family. Currently the gaussian and binomial families are supported. We support a subset of the available R formula operators for model fitting, including '~', '.', '+', and '-'. The example below shows the use of building a gaussian GLM model using SparkR.

<div data-lang="r"  markdown="1">
{% highlight r %}
# Create the DataFrame
df <- createDataFrame(sqlContext, iris)

# Fit a linear model over the dataset.
model <- glm(Sepal_Length ~ Sepal_Width + Species, data = df, family = "gaussian")

# Model coefficients are returned in a similar format to R's native glm().
summary(model)
##$coefficients
##                    Estimate
##(Intercept)        2.2513930
##Sepal_Width        0.8035609
##Species_versicolor 1.4587432
##Species_virginica  1.9468169

# Make predictions based on the model.
predictions <- predict(model, newData = df)
head(select(predictions, "Sepal_Length", "prediction"))
##  Sepal_Length prediction
##1          5.1   5.063856
##2          4.9   4.662076
##3          4.7   4.822788
##4          4.6   4.742432
##5          5.0   5.144212
##6          5.4   5.385281
{% endhighlight %}
</div>
