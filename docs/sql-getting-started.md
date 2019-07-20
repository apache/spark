---
layout: global
title: Getting Started
displayTitle: Getting Started
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

* Table of contents
{:toc}

## Starting Point: SparkSession

<div class="codetabs">
<div data-lang="scala"  markdown="1">

The entry point into all functionality in Spark is the [`SparkSession`](api/scala/index.html#org.apache.spark.sql.SparkSession) class. To create a basic `SparkSession`, just use `SparkSession.builder()`:

{% include_example init_session scala/org/apache/spark/examples/sql/SparkSQLExample.scala %}
</div>

<div data-lang="java" markdown="1">

The entry point into all functionality in Spark is the [`SparkSession`](api/java/index.html#org.apache.spark.sql.SparkSession) class. To create a basic `SparkSession`, just use `SparkSession.builder()`:

{% include_example init_session java/org/apache/spark/examples/sql/JavaSparkSQLExample.java %}
</div>

<div data-lang="python"  markdown="1">

The entry point into all functionality in Spark is the [`SparkSession`](api/python/pyspark.sql.html#pyspark.sql.SparkSession) class. To create a basic `SparkSession`, just use `SparkSession.builder`:

{% include_example init_session python/sql/basic.py %}
</div>

<div data-lang="r"  markdown="1">

The entry point into all functionality in Spark is the [`SparkSession`](api/R/sparkR.session.html) class. To initialize a basic `SparkSession`, just call `sparkR.session()`:

{% include_example init_session r/RSparkSQLExample.R %}

Note that when invoked for the first time, `sparkR.session()` initializes a global `SparkSession` singleton instance, and always returns a reference to this instance for successive invocations. In this way, users only need to initialize the `SparkSession` once, then SparkR functions like `read.df` will be able to access this global instance implicitly, and users don't need to pass the `SparkSession` instance around.
</div>
</div>

`SparkSession` in Spark 2.0 provides builtin support for Hive features including the ability to
write queries using HiveQL, access to Hive UDFs, and the ability to read data from Hive tables.
To use these features, you do not need to have an existing Hive setup.

## Creating DataFrames

<div class="codetabs">
<div data-lang="scala"  markdown="1">
With a `SparkSession`, applications can create DataFrames from an [existing `RDD`](#interoperating-with-rdds),
from a Hive table, or from [Spark data sources](sql-data-sources.html).

As an example, the following creates a DataFrame based on the content of a JSON file:

{% include_example create_df scala/org/apache/spark/examples/sql/SparkSQLExample.scala %}
</div>

<div data-lang="java" markdown="1">
With a `SparkSession`, applications can create DataFrames from an [existing `RDD`](#interoperating-with-rdds),
from a Hive table, or from [Spark data sources](sql-data-sources.html).

As an example, the following creates a DataFrame based on the content of a JSON file:

{% include_example create_df java/org/apache/spark/examples/sql/JavaSparkSQLExample.java %}
</div>

<div data-lang="python"  markdown="1">
With a `SparkSession`, applications can create DataFrames from an [existing `RDD`](#interoperating-with-rdds),
from a Hive table, or from [Spark data sources](sql-data-sources.html).

As an example, the following creates a DataFrame based on the content of a JSON file:

{% include_example create_df python/sql/basic.py %}
</div>

<div data-lang="r"  markdown="1">
With a `SparkSession`, applications can create DataFrames from a local R data.frame,
from a Hive table, or from [Spark data sources](sql-data-sources.html).

As an example, the following creates a DataFrame based on the content of a JSON file:

{% include_example create_df r/RSparkSQLExample.R %}

</div>
</div>


## Untyped Dataset Operations (aka DataFrame Operations)

DataFrames provide a domain-specific language for structured data manipulation in [Scala](api/scala/index.html#org.apache.spark.sql.Dataset), [Java](api/java/index.html?org/apache/spark/sql/Dataset.html), [Python](api/python/pyspark.sql.html#pyspark.sql.DataFrame) and [R](api/R/SparkDataFrame.html).

As mentioned above, in Spark 2.0, DataFrames are just Dataset of `Row`s in Scala and Java API. These operations are also referred as "untyped transformations" in contrast to "typed transformations" come with strongly typed Scala/Java Datasets.

Here we include some basic examples of structured data processing using Datasets:

<div class="codetabs">
<div data-lang="scala"  markdown="1">
{% include_example untyped_ops scala/org/apache/spark/examples/sql/SparkSQLExample.scala %}

For a complete list of the types of operations that can be performed on a Dataset, refer to the [API Documentation](api/scala/index.html#org.apache.spark.sql.Dataset).

In addition to simple column references and expressions, Datasets also have a rich library of functions including string manipulation, date arithmetic, common math operations and more. The complete list is available in the [DataFrame Function Reference](api/scala/index.html#org.apache.spark.sql.functions$).
</div>

<div data-lang="java" markdown="1">

{% include_example untyped_ops java/org/apache/spark/examples/sql/JavaSparkSQLExample.java %}

For a complete list of the types of operations that can be performed on a Dataset refer to the [API Documentation](api/java/org/apache/spark/sql/Dataset.html).

In addition to simple column references and expressions, Datasets also have a rich library of functions including string manipulation, date arithmetic, common math operations and more. The complete list is available in the [DataFrame Function Reference](api/java/org/apache/spark/sql/functions.html).
</div>

<div data-lang="python"  markdown="1">
In Python, it's possible to access a DataFrame's columns either by attribute
(`df.age`) or by indexing (`df['age']`). While the former is convenient for
interactive data exploration, users are highly encouraged to use the
latter form, which is future proof and won't break with column names that
are also attributes on the DataFrame class.

{% include_example untyped_ops python/sql/basic.py %}
For a complete list of the types of operations that can be performed on a DataFrame refer to the [API Documentation](api/python/pyspark.sql.html#pyspark.sql.DataFrame).

In addition to simple column references and expressions, DataFrames also have a rich library of functions including string manipulation, date arithmetic, common math operations and more. The complete list is available in the [DataFrame Function Reference](api/python/pyspark.sql.html#module-pyspark.sql.functions).

</div>

<div data-lang="r"  markdown="1">

{% include_example untyped_ops r/RSparkSQLExample.R %}

For a complete list of the types of operations that can be performed on a DataFrame refer to the [API Documentation](api/R/index.html).

In addition to simple column references and expressions, DataFrames also have a rich library of functions including string manipulation, date arithmetic, common math operations and more. The complete list is available in the [DataFrame Function Reference](api/R/SparkDataFrame.html).

</div>

</div>

## Running SQL Queries Programmatically

<div class="codetabs">
<div data-lang="scala"  markdown="1">
The `sql` function on a `SparkSession` enables applications to run SQL queries programmatically and returns the result as a `DataFrame`.

{% include_example run_sql scala/org/apache/spark/examples/sql/SparkSQLExample.scala %}
</div>

<div data-lang="java" markdown="1">
The `sql` function on a `SparkSession` enables applications to run SQL queries programmatically and returns the result as a `Dataset<Row>`.

{% include_example run_sql java/org/apache/spark/examples/sql/JavaSparkSQLExample.java %}
</div>

<div data-lang="python"  markdown="1">
The `sql` function on a `SparkSession` enables applications to run SQL queries programmatically and returns the result as a `DataFrame`.

{% include_example run_sql python/sql/basic.py %}
</div>

<div data-lang="r"  markdown="1">
The `sql` function enables applications to run SQL queries programmatically and returns the result as a `SparkDataFrame`.

{% include_example run_sql r/RSparkSQLExample.R %}

</div>
</div>


## Global Temporary View

Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it
terminates. If you want to have a temporary view that is shared among all sessions and keep alive
until the Spark application terminates, you can create a global temporary view. Global temporary
view is tied to a system preserved database `global_temp`, and we must use the qualified name to
refer it, e.g. `SELECT * FROM global_temp.view1`.

<div class="codetabs">
<div data-lang="scala"  markdown="1">
{% include_example global_temp_view scala/org/apache/spark/examples/sql/SparkSQLExample.scala %}
</div>

<div data-lang="java" markdown="1">
{% include_example global_temp_view java/org/apache/spark/examples/sql/JavaSparkSQLExample.java %}
</div>

<div data-lang="python"  markdown="1">
{% include_example global_temp_view python/sql/basic.py %}
</div>

<div data-lang="sql"  markdown="1">

{% highlight sql %}

CREATE GLOBAL TEMPORARY VIEW temp_view AS SELECT a + 1, b * 2 FROM tbl

SELECT * FROM global_temp.temp_view

{% endhighlight %}

</div>
</div>


## Creating Datasets

Datasets are similar to RDDs, however, instead of using Java serialization or Kryo they use
a specialized [Encoder](api/scala/index.html#org.apache.spark.sql.Encoder) to serialize the objects
for processing or transmitting over the network. While both encoders and standard serialization are
responsible for turning an object into bytes, encoders are code generated dynamically and use a format
that allows Spark to perform many operations like filtering, sorting and hashing without deserializing
the bytes back into an object.

<div class="codetabs">
<div data-lang="scala"  markdown="1">
{% include_example create_ds scala/org/apache/spark/examples/sql/SparkSQLExample.scala %}
</div>

<div data-lang="java" markdown="1">
{% include_example create_ds java/org/apache/spark/examples/sql/JavaSparkSQLExample.java %}
</div>
</div>

## Interoperating with RDDs

Spark SQL supports two different methods for converting existing RDDs into Datasets. The first
method uses reflection to infer the schema of an RDD that contains specific types of objects. This
reflection-based approach leads to more concise code and works well when you already know the schema
while writing your Spark application.

The second method for creating Datasets is through a programmatic interface that allows you to
construct a schema and then apply it to an existing RDD. While this method is more verbose, it allows
you to construct Datasets when the columns and their types are not known until runtime.

### Inferring the Schema Using Reflection
<div class="codetabs">

<div data-lang="scala"  markdown="1">

The Scala interface for Spark SQL supports automatically converting an RDD containing case classes
to a DataFrame. The case class
defines the schema of the table. The names of the arguments to the case class are read using
reflection and become the names of the columns. Case classes can also be nested or contain complex
types such as `Seq`s or `Array`s. This RDD can be implicitly converted to a DataFrame and then be
registered as a table. Tables can be used in subsequent SQL statements.

{% include_example schema_inferring scala/org/apache/spark/examples/sql/SparkSQLExample.scala %}
</div>

<div data-lang="java"  markdown="1">

Spark SQL supports automatically converting an RDD of
[JavaBeans](http://stackoverflow.com/questions/3295496/what-is-a-javabean-exactly) into a DataFrame.
The `BeanInfo`, obtained using reflection, defines the schema of the table. Currently, Spark SQL
does not support JavaBeans that contain `Map` field(s). Nested JavaBeans and `List` or `Array`
fields are supported though. You can create a JavaBean by creating a class that implements
Serializable and has getters and setters for all of its fields.

{% include_example schema_inferring java/org/apache/spark/examples/sql/JavaSparkSQLExample.java %}
</div>

<div data-lang="python"  markdown="1">

Spark SQL can convert an RDD of Row objects to a DataFrame, inferring the datatypes. Rows are constructed by passing a list of
key/value pairs as kwargs to the Row class. The keys of this list define the column names of the table,
and the types are inferred by sampling the whole dataset, similar to the inference that is performed on JSON files.

{% include_example schema_inferring python/sql/basic.py %}
</div>

</div>

### Programmatically Specifying the Schema

<div class="codetabs">

<div data-lang="scala"  markdown="1">

When case classes cannot be defined ahead of time (for example,
the structure of records is encoded in a string, or a text dataset will be parsed
and fields will be projected differently for different users),
a `DataFrame` can be created programmatically with three steps.

1. Create an RDD of `Row`s from the original RDD;
2. Create the schema represented by a `StructType` matching the structure of
`Row`s in the RDD created in Step 1.
3. Apply the schema to the RDD of `Row`s via `createDataFrame` method provided
by `SparkSession`.

For example:

{% include_example programmatic_schema scala/org/apache/spark/examples/sql/SparkSQLExample.scala %}
</div>

<div data-lang="java"  markdown="1">

When JavaBean classes cannot be defined ahead of time (for example,
the structure of records is encoded in a string, or a text dataset will be parsed and
fields will be projected differently for different users),
a `Dataset<Row>` can be created programmatically with three steps.

1. Create an RDD of `Row`s from the original RDD;
2. Create the schema represented by a `StructType` matching the structure of
`Row`s in the RDD created in Step 1.
3. Apply the schema to the RDD of `Row`s via `createDataFrame` method provided
by `SparkSession`.

For example:

{% include_example programmatic_schema java/org/apache/spark/examples/sql/JavaSparkSQLExample.java %}
</div>

<div data-lang="python"  markdown="1">

When a dictionary of kwargs cannot be defined ahead of time (for example,
the structure of records is encoded in a string, or a text dataset will be parsed and
fields will be projected differently for different users),
a `DataFrame` can be created programmatically with three steps.

1. Create an RDD of tuples or lists from the original RDD;
2. Create the schema represented by a `StructType` matching the structure of
tuples or lists in the RDD created in the step 1.
3. Apply the schema to the RDD via `createDataFrame` method provided by `SparkSession`.

For example:

{% include_example programmatic_schema python/sql/basic.py %}
</div>

</div>

## Aggregations

The [built-in DataFrames functions](api/scala/index.html#org.apache.spark.sql.functions$) provide common
aggregations such as `count()`, `countDistinct()`, `avg()`, `max()`, `min()`, etc.
While those functions are designed for DataFrames, Spark SQL also has type-safe versions for some of them in
[Scala](api/scala/index.html#org.apache.spark.sql.expressions.scalalang.typed$) and
[Java](api/java/org/apache/spark/sql/expressions/javalang/typed.html) to work with strongly typed Datasets.
Moreover, users are not limited to the predefined aggregate functions and can create their own.

### Untyped User-Defined Aggregate Functions
Users have to extend the [UserDefinedAggregateFunction](api/scala/index.html#org.apache.spark.sql.expressions.UserDefinedAggregateFunction)
abstract class to implement a custom untyped aggregate function. For example, a user-defined average
can look like:

<div class="codetabs">
<div data-lang="scala"  markdown="1">
{% include_example untyped_custom_aggregation scala/org/apache/spark/examples/sql/UserDefinedUntypedAggregation.scala%}
</div>
<div data-lang="java"  markdown="1">
{% include_example untyped_custom_aggregation java/org/apache/spark/examples/sql/JavaUserDefinedUntypedAggregation.java%}
</div>
</div>

### Type-Safe User-Defined Aggregate Functions

User-defined aggregations for strongly typed Datasets revolve around the [Aggregator](api/scala/index.html#org.apache.spark.sql.expressions.Aggregator) abstract class.
For example, a type-safe user-defined average can look like:

<div class="codetabs">
<div data-lang="scala"  markdown="1">
{% include_example typed_custom_aggregation scala/org/apache/spark/examples/sql/UserDefinedTypedAggregation.scala%}
</div>
<div data-lang="java"  markdown="1">
{% include_example typed_custom_aggregation java/org/apache/spark/examples/sql/JavaUserDefinedTypedAggregation.java%}
</div>
</div>
