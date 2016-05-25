---
layout: global
displayTitle: Spark SQL, DataFrames and Datasets Guide
title: Spark SQL and DataFrames
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

Spark SQL is a Spark module for structured data processing. Unlike the basic Spark RDD API, the interfaces provided
by Spark SQL provide Spark with more information about the structure of both the data and the computation being performed. Internally,
Spark SQL uses this extra information to perform extra optimizations. There are several ways to
interact with Spark SQL including SQL, the DataFrames API and the Datasets API. When computing a result
the same execution engine is used, independent of which API/language you are using to express the
computation. This unification means that developers can easily switch back and forth between the
various APIs based on which provides the most natural way to express a given transformation.

All of the examples on this page use sample data included in the Spark distribution and can be run in
the `spark-shell`, `pyspark` shell, or `sparkR` shell.

## SQL

One use of Spark SQL is to execute SQL queries written using either a basic SQL syntax or HiveQL.
Spark SQL can also be used to read data from an existing Hive installation. For more on how to
configure this feature, please refer to the [Hive Tables](#hive-tables) section. When running
SQL from within another programming language the results will be returned as a [DataFrame](#DataFrames).
You can also interact with the SQL interface using the [command-line](#running-the-spark-sql-cli)
or over [JDBC/ODBC](#running-the-thrift-jdbcodbc-server).

## DataFrames

A DataFrame is a distributed collection of data organized into named columns. It is conceptually
equivalent to a table in a relational database or a data frame in R/Python, but with richer
optimizations under the hood. DataFrames can be constructed from a wide array of [sources](#data-sources) such
as: structured data files, tables in Hive, external databases, or existing RDDs.

The DataFrame API is available in [Scala](api/scala/index.html#org.apache.spark.sql.DataFrame),
[Java](api/java/index.html?org/apache/spark/sql/DataFrame.html),
[Python](api/python/pyspark.sql.html#pyspark.sql.DataFrame), and [R](api/R/index.html).

## Datasets

A Dataset is a new experimental interface added in Spark 1.6 that tries to provide the benefits of
RDDs (strong typing, ability to use powerful lambda functions) with the benefits of Spark SQL's
optimized execution engine. A Dataset can be [constructed](#creating-datasets) from JVM objects and then manipulated
using functional transformations (map, flatMap, filter, etc.).

The unified Dataset API can be used both in [Scala](api/scala/index.html#org.apache.spark.sql.Dataset) and
[Java](api/java/index.html?org/apache/spark/sql/Dataset.html). Python does not yet have support for
the Dataset API, but due to its dynamic nature many of the benefits are already available (i.e. you can
access the field of a row by name naturally `row.columnName`). Full python support will be added
in a future release.

# Getting Started

## Starting Point: SQLContext

<div class="codetabs">
<div data-lang="scala"  markdown="1">

The entry point into all functionality in Spark SQL is the
[`SQLContext`](api/scala/index.html#org.apache.spark.sql.SQLContext) class, or one of its
descendants. To create a basic `SQLContext`, all you need is a SparkContext.

{% highlight scala %}
val sc: SparkContext // An existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
{% endhighlight %}

</div>

<div data-lang="java" markdown="1">

The entry point into all functionality in Spark SQL is the
[`SQLContext`](api/java/index.html#org.apache.spark.sql.SQLContext) class, or one of its
descendants. To create a basic `SQLContext`, all you need is a SparkContext.

{% highlight java %}
JavaSparkContext sc = ...; // An existing JavaSparkContext.
SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

The entry point into all relational functionality in Spark is the
[`SQLContext`](api/python/pyspark.sql.html#pyspark.sql.SQLContext) class, or one
of its decedents. To create a basic `SQLContext`, all you need is a SparkContext.

{% highlight python %}
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
{% endhighlight %}

</div>

<div data-lang="r"  markdown="1">

The entry point into all relational functionality in Spark is the
`SQLContext` class, or one of its decedents. To create a basic `SQLContext`, all you need is a SparkContext.

{% highlight r %}
sqlContext <- sparkRSQL.init(sc)
{% endhighlight %}

</div>
</div>

In addition to the basic `SQLContext`, you can also create a `HiveContext`, which provides a
superset of the functionality provided by the basic `SQLContext`. Additional features include
the ability to write queries using the more complete HiveQL parser, access to Hive UDFs, and the
ability to read data from Hive tables. To use a `HiveContext`, you do not need to have an
existing Hive setup, and all of the data sources available to a `SQLContext` are still available.
`HiveContext` is only packaged separately to avoid including all of Hive's dependencies in the default
Spark build. If these dependencies are not a problem for your application then using `HiveContext`
is recommended for the 1.3 release of Spark. Future releases will focus on bringing `SQLContext` up
to feature parity with a `HiveContext`.


## Creating DataFrames

With a `SQLContext`, applications can create `DataFrame`s from an <a href='#interoperating-with-rdds'>existing `RDD`</a>, from a Hive table, or from <a href='#data-sources'>data sources</a>.

As an example, the following creates a `DataFrame` based on the content of a JSON file:

<div class="codetabs">
<div data-lang="scala"  markdown="1">
{% highlight scala %}
val sc: SparkContext // An existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val df = sqlContext.read.json("examples/src/main/resources/people.json")

// Displays the content of the DataFrame to stdout
df.show()
{% endhighlight %}

</div>

<div data-lang="java" markdown="1">
{% highlight java %}
JavaSparkContext sc = ...; // An existing JavaSparkContext.
SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

DataFrame df = sqlContext.read().json("examples/src/main/resources/people.json");

// Displays the content of the DataFrame to stdout
df.show();
{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.json("examples/src/main/resources/people.json")

# Displays the content of the DataFrame to stdout
df.show()
{% endhighlight %}

</div>

<div data-lang="r"  markdown="1">
{% highlight r %}
sqlContext <- SQLContext(sc)

df <- read.json(sqlContext, "examples/src/main/resources/people.json")

# Displays the content of the DataFrame to stdout
showDF(df)
{% endhighlight %}

</div>

</div>


## DataFrame Operations

DataFrames provide a domain-specific language for structured data manipulation in [Scala](api/scala/index.html#org.apache.spark.sql.DataFrame), [Java](api/java/index.html?org/apache/spark/sql/DataFrame.html), [Python](api/python/pyspark.sql.html#pyspark.sql.DataFrame) and [R](api/R/DataFrame.html).

Here we include some basic examples of structured data processing using DataFrames:

<div class="codetabs">
<div data-lang="scala"  markdown="1">
{% highlight scala %}
val sc: SparkContext // An existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// Create the DataFrame
val df = sqlContext.read.json("examples/src/main/resources/people.json")

// Show the content of the DataFrame
df.show()
// age  name
// null Michael
// 30   Andy
// 19   Justin

// Print the schema in a tree format
df.printSchema()
// root
// |-- age: long (nullable = true)
// |-- name: string (nullable = true)

// Select only the "name" column
df.select("name").show()
// name
// Michael
// Andy
// Justin

// Select everybody, but increment the age by 1
df.select(df("name"), df("age") + 1).show()
// name    (age + 1)
// Michael null
// Andy    31
// Justin  20

// Select people older than 21
df.filter(df("age") > 21).show()
// age name
// 30  Andy

// Count people by age
df.groupBy("age").count().show()
// age  count
// null 1
// 19   1
// 30   1
{% endhighlight %}

For a complete list of the types of operations that can be performed on a DataFrame refer to the [API Documentation](api/scala/index.html#org.apache.spark.sql.DataFrame).

In addition to simple column references and expressions, DataFrames also have a rich library of functions including string manipulation, date arithmetic, common math operations and more. The complete list is available in the [DataFrame Function Reference](api/scala/index.html#org.apache.spark.sql.functions$).


</div>

<div data-lang="java" markdown="1">
{% highlight java %}
JavaSparkContext sc // An existing SparkContext.
SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc)

// Create the DataFrame
DataFrame df = sqlContext.read().json("examples/src/main/resources/people.json");

// Show the content of the DataFrame
df.show();
// age  name
// null Michael
// 30   Andy
// 19   Justin

// Print the schema in a tree format
df.printSchema();
// root
// |-- age: long (nullable = true)
// |-- name: string (nullable = true)

// Select only the "name" column
df.select("name").show();
// name
// Michael
// Andy
// Justin

// Select everybody, but increment the age by 1
df.select(df.col("name"), df.col("age").plus(1)).show();
// name    (age + 1)
// Michael null
// Andy    31
// Justin  20

// Select people older than 21
df.filter(df.col("age").gt(21)).show();
// age name
// 30  Andy

// Count people by age
df.groupBy("age").count().show();
// age  count
// null 1
// 19   1
// 30   1
{% endhighlight %}

For a complete list of the types of operations that can be performed on a DataFrame refer to the [API Documentation](api/java/org/apache/spark/sql/DataFrame.html).

In addition to simple column references and expressions, DataFrames also have a rich library of functions including string manipulation, date arithmetic, common math operations and more. The complete list is available in the [DataFrame Function Reference](api/java/org/apache/spark/sql/functions.html).

</div>

<div data-lang="python"  markdown="1">
In Python it's possible to access a DataFrame's columns either by attribute
(`df.age`) or by indexing (`df['age']`). While the former is convenient for
interactive data exploration, users are highly encouraged to use the
latter form, which is future proof and won't break with column names that
are also attributes on the DataFrame class.

{% highlight python %}
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# Create the DataFrame
df = sqlContext.read.json("examples/src/main/resources/people.json")

# Show the content of the DataFrame
df.show()
## age  name
## null Michael
## 30   Andy
## 19   Justin

# Print the schema in a tree format
df.printSchema()
## root
## |-- age: long (nullable = true)
## |-- name: string (nullable = true)

# Select only the "name" column
df.select("name").show()
## name
## Michael
## Andy
## Justin

# Select everybody, but increment the age by 1
df.select(df['name'], df['age'] + 1).show()
## name    (age + 1)
## Michael null
## Andy    31
## Justin  20

# Select people older than 21
df.filter(df['age'] > 21).show()
## age name
## 30  Andy

# Count people by age
df.groupBy("age").count().show()
## age  count
## null 1
## 19   1
## 30   1

{% endhighlight %}

For a complete list of the types of operations that can be performed on a DataFrame refer to the [API Documentation](api/python/pyspark.sql.html#pyspark.sql.DataFrame).

In addition to simple column references and expressions, DataFrames also have a rich library of functions including string manipulation, date arithmetic, common math operations and more. The complete list is available in the [DataFrame Function Reference](api/python/pyspark.sql.html#module-pyspark.sql.functions).

</div>

<div data-lang="r"  markdown="1">
{% highlight r %}
sqlContext <- sparkRSQL.init(sc)

# Create the DataFrame
df <- read.json(sqlContext, "examples/src/main/resources/people.json")

# Show the content of the DataFrame
showDF(df)
## age  name
## null Michael
## 30   Andy
## 19   Justin

# Print the schema in a tree format
printSchema(df)
## root
## |-- age: long (nullable = true)
## |-- name: string (nullable = true)

# Select only the "name" column
showDF(select(df, "name"))
## name
## Michael
## Andy
## Justin

# Select everybody, but increment the age by 1
showDF(select(df, df$name, df$age + 1))
## name    (age + 1)
## Michael null
## Andy    31
## Justin  20

# Select people older than 21
showDF(where(df, df$age > 21))
## age name
## 30  Andy

# Count people by age
showDF(count(groupBy(df, "age")))
## age  count
## null 1
## 19   1
## 30   1

{% endhighlight %}

For a complete list of the types of operations that can be performed on a DataFrame refer to the [API Documentation](api/R/index.html).

In addition to simple column references and expressions, DataFrames also have a rich library of functions including string manipulation, date arithmetic, common math operations and more. The complete list is available in the [DataFrame Function Reference](api/R/index.html).

</div>

</div>

## Running SQL Queries Programmatically

The `sql` function on a `SQLContext` enables applications to run SQL queries programmatically and returns the result as a `DataFrame`.

<div class="codetabs">
<div data-lang="scala"  markdown="1">
{% highlight scala %}
val sqlContext = ... // An existing SQLContext
val df = sqlContext.sql("SELECT * FROM table")
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
SQLContext sqlContext = ... // An existing SQLContext
DataFrame df = sqlContext.sql("SELECT * FROM table")
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df = sqlContext.sql("SELECT * FROM table")
{% endhighlight %}
</div>

<div data-lang="r"  markdown="1">
{% highlight r %}
sqlContext <- sparkRSQL.init(sc)
df <- sql(sqlContext, "SELECT * FROM table")
{% endhighlight %}
</div>

</div>


## Creating Datasets

Datasets are similar to RDDs, however, instead of using Java Serialization or Kryo they use
a specialized [Encoder](api/scala/index.html#org.apache.spark.sql.Encoder) to serialize the objects
for processing or transmitting over the network. While both encoders and standard serialization are
responsible for turning an object into bytes, encoders are code generated dynamically and use a format
that allows Spark to perform many operations like filtering, sorting and hashing without deserializing
the bytes back into an object.

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
// Encoders for most common types are automatically provided by importing sqlContext.implicits._
val ds = Seq(1, 2, 3).toDS()
ds.map(_ + 1).collect() // Returns: Array(2, 3, 4)

// Encoders are also created for case classes.
case class Person(name: String, age: Long)
val ds = Seq(Person("Andy", 32)).toDS()

// DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name.
val path = "examples/src/main/resources/people.json"
val people = sqlContext.read.json(path).as[Person]

{% endhighlight %}

</div>

<div data-lang="java" markdown="1">

{% highlight java %}
JavaSparkContext sc = ...; // An existing JavaSparkContext.
SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
{% endhighlight %}

</div>
</div>

## Interoperating with RDDs

Spark SQL supports two different methods for converting existing RDDs into DataFrames. The first
method uses reflection to infer the schema of an RDD that contains specific types of objects. This
reflection based approach leads to more concise code and works well when you already know the schema
while writing your Spark application.

The second method for creating DataFrames is through a programmatic interface that allows you to
construct a schema and then apply it to an existing RDD. While this method is more verbose, it allows
you to construct DataFrames when the columns and their types are not known until runtime.

### Inferring the Schema Using Reflection
<div class="codetabs">

<div data-lang="scala"  markdown="1">

The Scala interface for Spark SQL supports automatically converting an RDD containing case classes
to a DataFrame. The case class
defines the schema of the table. The names of the arguments to the case class are read using
reflection and become the names of the columns. Case classes can also be nested or contain complex
types such as Sequences or Arrays. This RDD can be implicitly converted to a DataFrame and then be
registered as a table. Tables can be used in subsequent SQL statements.

{% highlight scala %}
// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._

// Define the schema using a case class.
// Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
// you can use custom classes that implement the Product interface.
case class Person(name: String, age: Int)

// Create an RDD of Person objects and register it as a table.
val people = sc.textFile("examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
people.createOrReplaceTempView("people")

// SQL statements can be run by using the sql methods provided by sqlContext.
val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")

// The results of SQL queries are DataFrames and support all the normal RDD operations.
// The columns of a row in the result can be accessed by field index:
teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

// or by field name:
teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)

// row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
// Map("name" -> "Justin", "age" -> 19)
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

Spark SQL supports automatically converting an RDD of [JavaBeans](http://stackoverflow.com/questions/3295496/what-is-a-javabean-exactly)
into a DataFrame. The BeanInfo, obtained using reflection, defines the schema of the table.
Currently, Spark SQL does not support JavaBeans that contain
nested or contain complex types such as Lists or Arrays. You can create a JavaBean by creating a
class that implements Serializable and has getters and setters for all of its fields.

{% highlight java %}

public static class Person implements Serializable {
  private String name;
  private int age;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }
}

{% endhighlight %}


A schema can be applied to an existing RDD by calling `createDataFrame` and providing the Class object
for the JavaBean.

{% highlight java %}
// sc is an existing JavaSparkContext.
SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

// Load a text file and convert each line to a JavaBean.
JavaRDD<Person> people = sc.textFile("examples/src/main/resources/people.txt").map(
  new Function<String, Person>() {
    public Person call(String line) throws Exception {
      String[] parts = line.split(",");

      Person person = new Person();
      person.setName(parts[0]);
      person.setAge(Integer.parseInt(parts[1].trim()));

      return person;
    }
  });

// Apply a schema to an RDD of JavaBeans and register it as a table.
DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
schemaPeople.createOrReplaceTempView("people");

// SQL can be run over RDDs that have been registered as tables.
DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

// The results of SQL queries are DataFrames and support all the normal RDD operations.
// The columns of a row in the result can be accessed by ordinal.
List<String> teenagerNames = teenagers.javaRDD().map(new Function<Row, String>() {
  public String call(Row row) {
    return "Name: " + row.getString(0);
  }
}).collect();

{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

Spark SQL can convert an RDD of Row objects to a DataFrame, inferring the datatypes. Rows are constructed by passing a list of
key/value pairs as kwargs to the Row class. The keys of this list define the column names of the table,
and the types are inferred by looking at the first row. Since we currently only look at the first
row, it is important that there is no missing data in the first row of the RDD. In future versions we
plan to more completely infer the schema by looking at more data, similar to the inference that is
performed on JSON files.

{% highlight python %}
# sc is an existing SparkContext.
from pyspark.sql import SQLContext, Row
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a Row.
lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = sqlContext.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
teenNames = teenagers.map(lambda p: "Name: " + p.name)
for teenName in teenNames.collect():
  print(teenName)
{% endhighlight %}

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
by `SQLContext`.

For example:
{% highlight scala %}
// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// Create an RDD
val people = sc.textFile("examples/src/main/resources/people.txt")

// The schema is encoded in a string
val schemaString = "name age"

// Import Row.
import org.apache.spark.sql.Row;

// Import Spark SQL data types
import org.apache.spark.sql.types.{StructType,StructField,StringType};

// Generate the schema based on the string of schema
val schema =
  StructType(
    schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

// Convert records of the RDD (people) to Rows.
val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

// Apply the schema to the RDD.
val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

// Creates a temporary view using the DataFrame.
peopleDataFrame.createOrReplaceTempView("people")

// SQL statements can be run by using the sql methods provided by sqlContext.
val results = sqlContext.sql("SELECT name FROM people")

// The results of SQL queries are DataFrames and support all the normal RDD operations.
// The columns of a row in the result can be accessed by field index or by field name.
results.map(t => "Name: " + t(0)).collect().foreach(println)
{% endhighlight %}


</div>

<div data-lang="java"  markdown="1">

When JavaBean classes cannot be defined ahead of time (for example,
the structure of records is encoded in a string, or a text dataset will be parsed and
fields will be projected differently for different users),
a `DataFrame` can be created programmatically with three steps.

1. Create an RDD of `Row`s from the original RDD;
2. Create the schema represented by a `StructType` matching the structure of
`Row`s in the RDD created in Step 1.
3. Apply the schema to the RDD of `Row`s via `createDataFrame` method provided
by `SQLContext`.

For example:
{% highlight java %}
import org.apache.spark.api.java.function.Function;
// Import factory methods provided by DataTypes.
import org.apache.spark.sql.types.DataTypes;
// Import StructType and StructField
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
// Import Row.
import org.apache.spark.sql.Row;
// Import RowFactory.
import org.apache.spark.sql.RowFactory;

// sc is an existing JavaSparkContext.
SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

// Load a text file and convert each line to a JavaBean.
JavaRDD<String> people = sc.textFile("examples/src/main/resources/people.txt");

// The schema is encoded in a string
String schemaString = "name age";

// Generate the schema based on the string of schema
List<StructField> fields = new ArrayList<>();
for (String fieldName: schemaString.split(" ")) {
  fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
}
StructType schema = DataTypes.createStructType(fields);

// Convert records of the RDD (people) to Rows.
JavaRDD<Row> rowRDD = people.map(
  new Function<String, Row>() {
    public Row call(String record) throws Exception {
      String[] fields = record.split(",");
      return RowFactory.create(fields[0], fields[1].trim());
    }
  });

// Apply the schema to the RDD.
DataFrame peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);

// Creates a temporary view using the DataFrame.
peopleDataFrame.createOrReplaceTempView("people");

// SQL can be run over a temporary view created using DataFrames.
DataFrame results = sqlContext.sql("SELECT name FROM people");

// The results of SQL queries are DataFrames and support all the normal RDD operations.
// The columns of a row in the result can be accessed by ordinal.
List<String> names = results.javaRDD().map(new Function<Row, String>() {
  public String call(Row row) {
    return "Name: " + row.getString(0);
  }
}).collect();

{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

When a dictionary of kwargs cannot be defined ahead of time (for example,
the structure of records is encoded in a string, or a text dataset will be parsed and
fields will be projected differently for different users),
a `DataFrame` can be created programmatically with three steps.

1. Create an RDD of tuples or lists from the original RDD;
2. Create the schema represented by a `StructType` matching the structure of
tuples or lists in the RDD created in the step 1.
3. Apply the schema to the RDD via `createDataFrame` method provided by `SQLContext`.

For example:
{% highlight python %}
# Import SQLContext and data types
from pyspark.sql import SQLContext
from pyspark.sql.types import *

# sc is an existing SparkContext.
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a tuple.
lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: (p[0], p[1].strip()))

# The schema is encoded in a string.
schemaString = "name age"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaPeople = sqlContext.createDataFrame(people, schema)

# Creates a temporary view using the DataFrame
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
results = sqlContext.sql("SELECT name FROM people")

# The results of SQL queries are RDDs and support all the normal RDD operations.
names = results.map(lambda p: "Name: " + p.name)
for name in names.collect():
  print(name)
{% endhighlight %}

</div>

</div>


# Data Sources

Spark SQL supports operating on a variety of data sources through the `DataFrame` interface.
A DataFrame can be operated on as normal RDDs and can also be used to create a temporary view.
Registering a DataFrame as a table allows you to run SQL queries over its data. This section
describes the general methods for loading and saving data using the Spark Data Sources and then
goes into specific options that are available for the built-in data sources.

## Generic Load/Save Functions

In the simplest form, the default data source (`parquet` unless otherwise configured by
`spark.sql.sources.default`) will be used for all operations.

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
val df = sqlContext.read.load("examples/src/main/resources/users.parquet")
df.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}

DataFrame df = sqlContext.read().load("examples/src/main/resources/users.parquet");
df.select("name", "favorite_color").write().save("namesAndFavColors.parquet");

{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

{% highlight python %}

df = sqlContext.read.load("examples/src/main/resources/users.parquet")
df.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

{% endhighlight %}

</div>

<div data-lang="r"  markdown="1">

{% highlight r %}
df <- read.df(sqlContext, "examples/src/main/resources/users.parquet")
write.df(select(df, "name", "favorite_color"), "namesAndFavColors.parquet")
{% endhighlight %}

</div>
</div>

### Manually Specifying Options

You can also manually specify the data source that will be used along with any extra options
that you would like to pass to the data source. Data sources are specified by their fully qualified
name (i.e., `org.apache.spark.sql.parquet`), but for built-in sources you can also use their short
names (`json`, `parquet`, `jdbc`). DataFrames of any type can be converted into other types
using this syntax.

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
val df = sqlContext.read.format("json").load("examples/src/main/resources/people.json")
df.select("name", "age").write.format("parquet").save("namesAndAges.parquet")
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}

DataFrame df = sqlContext.read().format("json").load("examples/src/main/resources/people.json");
df.select("name", "age").write().format("parquet").save("namesAndAges.parquet");

{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

{% highlight python %}

df = sqlContext.read.load("examples/src/main/resources/people.json", format="json")
df.select("name", "age").write.save("namesAndAges.parquet", format="parquet")

{% endhighlight %}

</div>
<div data-lang="r"  markdown="1">

{% highlight r %}

df <- read.df(sqlContext, "examples/src/main/resources/people.json", "json")
write.df(select(df, "name", "age"), "namesAndAges.parquet", "parquet")

{% endhighlight %}

</div>
</div>

### Run SQL on files directly

Instead of using read API to load a file into DataFrame and query it, you can also query that
file directly with SQL.

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
val df = sqlContext.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}
DataFrame df = sqlContext.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`");
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">

{% highlight python %}
df = sqlContext.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")
{% endhighlight %}

</div>

<div data-lang="r"  markdown="1">

{% highlight r %}
df <- sql(sqlContext, "SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")
{% endhighlight %}

</div>
</div>

### Save Modes

Save operations can optionally take a `SaveMode`, that specifies how to handle existing data if
present. It is important to realize that these save modes do not utilize any locking and are not
atomic. Additionally, when performing a `Overwrite`, the data will be deleted before writing out the
new data.

<table class="table">
<tr><th>Scala/Java</th><th>Any Language</th><th>Meaning</th></tr>
<tr>
  <td><code>SaveMode.ErrorIfExists</code> (default)</td>
  <td><code>"error"</code> (default)</td>
  <td>
    When saving a DataFrame to a data source, if data already exists,
    an exception is expected to be thrown.
  </td>
</tr>
<tr>
  <td><code>SaveMode.Append</code></td>
  <td><code>"append"</code></td>
  <td>
    When saving a DataFrame to a data source, if data/table already exists,
    contents of the DataFrame are expected to be appended to existing data.
  </td>
</tr>
<tr>
  <td><code>SaveMode.Overwrite</code></td>
  <td><code>"overwrite"</code></td>
  <td>
    Overwrite mode means that when saving a DataFrame to a data source,
    if data/table already exists, existing data is expected to be overwritten by the contents of
    the DataFrame.
  </td>
</tr>
<tr>
  <td><code>SaveMode.Ignore</code></td>
  <td><code>"ignore"</code></td>
  <td>
    Ignore mode means that when saving a DataFrame to a data source, if data already exists,
    the save operation is expected to not save the contents of the DataFrame and to not
    change the existing data. This is similar to a <code>CREATE TABLE IF NOT EXISTS</code> in SQL.
  </td>
</tr>
</table>

### Saving to Persistent Tables

When working with a `HiveContext`, `DataFrames` can also be saved as persistent tables using the
`saveAsTable` command. Unlike the `registerTempTable` command, `saveAsTable` will materialize the
contents of the dataframe and create a pointer to the data in the HiveMetastore. Persistent tables
will still exist even after your Spark program has restarted, as long as you maintain your connection
to the same metastore. A DataFrame for a persistent table can be created by calling the `table`
method on a `SQLContext` with the name of the table.

By default `saveAsTable` will create a "managed table", meaning that the location of the data will
be controlled by the metastore. Managed tables will also have their data deleted automatically
when a table is dropped.

## Parquet Files

[Parquet](http://parquet.io) is a columnar format that is supported by many other data processing systems.
Spark SQL provides support for both reading and writing Parquet files that automatically preserves the schema
of the original data. When writing Parquet files, all columns are automatically converted to be nullable for 
compatibility reasons.

### Loading Data Programmatically

Using the data from the above example:

<div class="codetabs">

<div data-lang="scala"  markdown="1">

{% highlight scala %}
// sqlContext from the previous example is used in this example.
// This is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._

val people: RDD[Person] = ... // An RDD of case class objects, from the previous example.

// The RDD is implicitly converted to a DataFrame by implicits, allowing it to be stored using Parquet.
people.write.parquet("people.parquet")

// Read in the parquet file created above. Parquet files are self-describing so the schema is preserved.
// The result of loading a Parquet file is also a DataFrame.
val parquetFile = sqlContext.read.parquet("people.parquet")

// Parquet files can also be used to create a temporary view and then used in SQL statements.
parquetFile.createOrReplaceTempView("parquetFile")
val teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}
// sqlContext from the previous example is used in this example.

DataFrame schemaPeople = ... // The DataFrame from the previous example.

// DataFrames can be saved as Parquet files, maintaining the schema information.
schemaPeople.write().parquet("people.parquet");

// Read in the Parquet file created above. Parquet files are self-describing so the schema is preserved.
// The result of loading a parquet file is also a DataFrame.
DataFrame parquetFile = sqlContext.read().parquet("people.parquet");

// Parquet files can also be used to create a temporary view and then used in SQL statements.
parquetFile.createOrReplaceTempView("parquetFile");
DataFrame teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19");
List<String> teenagerNames = teenagers.javaRDD().map(new Function<Row, String>() {
  public String call(Row row) {
    return "Name: " + row.getString(0);
  }
}).collect();
{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

{% highlight python %}
# sqlContext from the previous example is used in this example.

schemaPeople # The DataFrame from the previous example.

# DataFrames can be saved as Parquet files, maintaining the schema information.
schemaPeople.write.parquet("people.parquet")

# Read in the Parquet file created above. Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a DataFrame.
parquetFile = sqlContext.read.parquet("people.parquet")

# Parquet files can also be used to create a temporary view and then used in SQL statements.
parquetFile.createOrReplaceTempView("parquetFile");
teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
teenNames = teenagers.map(lambda p: "Name: " + p.name)
for teenName in teenNames.collect():
  print(teenName)
{% endhighlight %}

</div>

<div data-lang="r"  markdown="1">

{% highlight r %}
# sqlContext from the previous example is used in this example.

schemaPeople # The DataFrame from the previous example.

# DataFrames can be saved as Parquet files, maintaining the schema information.
write.parquet(schemaPeople, "people.parquet")

# Read in the Parquet file created above. Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a DataFrame.
parquetFile <- read.parquet(sqlContext, "people.parquet")

# Parquet files can also be used to create a temporary view and then used in SQL statements.
registerTempTable(parquetFile, "parquetFile")
teenagers <- sql(sqlContext, "SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
schema <- structType(structField("name", "string"))
teenNames <- dapply(df, function(p) { cbind(paste("Name:", p$name)) }, schema)
for (teenName in collect(teenNames)$name) {
  cat(teenName, "\n")
}
{% endhighlight %}

</div>

<div data-lang="sql"  markdown="1">

{% highlight sql %}

CREATE TEMPORARY TABLE parquetTable
USING org.apache.spark.sql.parquet
OPTIONS (
  path "examples/src/main/resources/people.parquet"
)

SELECT * FROM parquetTable

{% endhighlight %}

</div>

</div>

### Partition Discovery

Table partitioning is a common optimization approach used in systems like Hive. In a partitioned
table, data are usually stored in different directories, with partitioning column values encoded in
the path of each partition directory. The Parquet data source is now able to discover and infer
partitioning information automatically. For example, we can store all our previously used
population data into a partitioned table using the following directory structure, with two extra
columns, `gender` and `country` as partitioning columns:

{% highlight text %}

path
└── to
    └── table
        ├── gender=male
        │   ├── ...
        │   │
        │   ├── country=US
        │   │   └── data.parquet
        │   ├── country=CN
        │   │   └── data.parquet
        │   └── ...
        └── gender=female
            ├── ...
            │
            ├── country=US
            │   └── data.parquet
            ├── country=CN
            │   └── data.parquet
            └── ...

{% endhighlight %}

By passing `path/to/table` to either `SQLContext.read.parquet` or `SQLContext.read.load`, Spark SQL
will automatically extract the partitioning information from the paths.
Now the schema of the returned DataFrame becomes:

{% highlight text %}

root
|-- name: string (nullable = true)
|-- age: long (nullable = true)
|-- gender: string (nullable = true)
|-- country: string (nullable = true)

{% endhighlight %}

Notice that the data types of the partitioning columns are automatically inferred. Currently,
numeric data types and string type are supported. Sometimes users may not want to automatically
infer the data types of the partitioning columns. For these use cases, the automatic type inference
can be configured by `spark.sql.sources.partitionColumnTypeInference.enabled`, which is default to
`true`. When type inference is disabled, string type will be used for the partitioning columns.

Starting from Spark 1.6.0, partition discovery only finds partitions under the given paths
by default. For the above example, if users pass `path/to/table/gender=male` to either 
`SQLContext.read.parquet` or `SQLContext.read.load`, `gender` will not be considered as a
partitioning column. If users need to specify the base path that partition discovery
should start with, they can set `basePath` in the data source options. For example,
when `path/to/table/gender=male` is the path of the data and
users set `basePath` to `path/to/table/`, `gender` will be a partitioning column.

### Schema Merging

Like ProtocolBuffer, Avro, and Thrift, Parquet also supports schema evolution. Users can start with
a simple schema, and gradually add more columns to the schema as needed. In this way, users may end
up with multiple Parquet files with different but mutually compatible schemas. The Parquet data
source is now able to automatically detect this case and merge schemas of all these files.

Since schema merging is a relatively expensive operation, and is not a necessity in most cases, we
turned it off by default starting from 1.5.0. You may enable it by

1. setting data source option `mergeSchema` to `true` when reading Parquet files (as shown in the
   examples below), or
2. setting the global SQL option `spark.sql.parquet.mergeSchema` to `true`.

<div class="codetabs">

<div data-lang="scala"  markdown="1">

{% highlight scala %}
// sqlContext from the previous example is used in this example.
// This is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._

// Create a simple DataFrame, stored into a partition directory
val df1 = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
df1.write.parquet("data/test_table/key=1")

// Create another DataFrame in a new partition directory,
// adding a new column and dropping an existing column
val df2 = sc.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
df2.write.parquet("data/test_table/key=2")

// Read the partitioned table
val df3 = sqlContext.read.option("mergeSchema", "true").parquet("data/test_table")
df3.printSchema()

// The final schema consists of all 3 columns in the Parquet files together
// with the partitioning column appeared in the partition directory paths.
// root
// |-- single: int (nullable = true)
// |-- double: int (nullable = true)
// |-- triple: int (nullable = true)
// |-- key : int (nullable = true)
{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

{% highlight python %}
# sqlContext from the previous example is used in this example.

# Create a simple DataFrame, stored into a partition directory
df1 = sqlContext.createDataFrame(sc.parallelize(range(1, 6))\
                                   .map(lambda i: Row(single=i, double=i * 2)))
df1.write.parquet("data/test_table/key=1")

# Create another DataFrame in a new partition directory,
# adding a new column and dropping an existing column
df2 = sqlContext.createDataFrame(sc.parallelize(range(6, 11))
                                   .map(lambda i: Row(single=i, triple=i * 3)))
df2.write.parquet("data/test_table/key=2")

# Read the partitioned table
df3 = sqlContext.read.option("mergeSchema", "true").parquet("data/test_table")
df3.printSchema()

# The final schema consists of all 3 columns in the Parquet files together
# with the partitioning column appeared in the partition directory paths.
# root
# |-- single: int (nullable = true)
# |-- double: int (nullable = true)
# |-- triple: int (nullable = true)
# |-- key : int (nullable = true)
{% endhighlight %}

</div>

<div data-lang="r"  markdown="1">

{% highlight r %}
# sqlContext from the previous example is used in this example.

# Create a simple DataFrame, stored into a partition directory
write.df(df1, "data/test_table/key=1", "parquet", "overwrite")

# Create another DataFrame in a new partition directory,
# adding a new column and dropping an existing column
write.df(df2, "data/test_table/key=2", "parquet", "overwrite")

# Read the partitioned table
df3 <- read.df(sqlContext, "data/test_table", "parquet", mergeSchema="true")
printSchema(df3)

# The final schema consists of all 3 columns in the Parquet files together
# with the partitioning column appeared in the partition directory paths.
# root
# |-- single: int (nullable = true)
# |-- double: int (nullable = true)
# |-- triple: int (nullable = true)
# |-- key : int (nullable = true)
{% endhighlight %}

</div>

</div>

### Hive metastore Parquet table conversion

When reading from and writing to Hive metastore Parquet tables, Spark SQL will try to use its own
Parquet support instead of Hive SerDe for better performance. This behavior is controlled by the
`spark.sql.hive.convertMetastoreParquet` configuration, and is turned on by default.

#### Hive/Parquet Schema Reconciliation

There are two key differences between Hive and Parquet from the perspective of table schema
processing.

1. Hive is case insensitive, while Parquet is not
1. Hive considers all columns nullable, while nullability in Parquet is significant

Due to this reason, we must reconcile Hive metastore schema with Parquet schema when converting a
Hive metastore Parquet table to a Spark SQL Parquet table. The reconciliation rules are:

1. Fields that have the same name in both schema must have the same data type regardless of
   nullability. The reconciled field should have the data type of the Parquet side, so that
   nullability is respected.

1. The reconciled schema contains exactly those fields defined in Hive metastore schema.

   - Any fields that only appear in the Parquet schema are dropped in the reconciled schema.
   - Any fields that only appear in the Hive metastore schema are added as nullable field in the
     reconciled schema.

#### Metadata Refreshing

Spark SQL caches Parquet metadata for better performance. When Hive metastore Parquet table
conversion is enabled, metadata of those converted tables are also cached. If these tables are
updated by Hive or other external tools, you need to refresh them manually to ensure consistent
metadata.

<div class="codetabs">

<div data-lang="scala"  markdown="1">

{% highlight scala %}
// sqlContext is an existing HiveContext
sqlContext.refreshTable("my_table")
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}
// sqlContext is an existing HiveContext
sqlContext.refreshTable("my_table")
{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

{% highlight python %}
# sqlContext is an existing HiveContext
sqlContext.refreshTable("my_table")
{% endhighlight %}

</div>

<div data-lang="sql"  markdown="1">

{% highlight sql %}
REFRESH TABLE my_table;
{% endhighlight %}

</div>

</div>

### Configuration

Configuration of Parquet can be done using the `setConf` method on `SQLContext` or by running
`SET key=value` commands using SQL.

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.sql.parquet.binaryAsString</code></td>
  <td>false</td>
  <td>
    Some other Parquet-producing systems, in particular Impala, Hive, and older versions of Spark SQL, do
    not differentiate between binary data and strings when writing out the Parquet schema. This
    flag tells Spark SQL to interpret binary data as a string to provide compatibility with these systems.
  </td>
</tr>
<tr>
  <td><code>spark.sql.parquet.int96AsTimestamp</code></td>
  <td>true</td>
  <td>
    Some Parquet-producing systems, in particular Impala and Hive, store Timestamp into INT96. This
    flag tells Spark SQL to interpret INT96 data as a timestamp to provide compatibility with these systems.
  </td>
</tr>
<tr>
  <td><code>spark.sql.parquet.cacheMetadata</code></td>
  <td>true</td>
  <td>
    Turns on caching of Parquet schema metadata. Can speed up querying of static data.
  </td>
</tr>
<tr>
  <td><code>spark.sql.parquet.compression.codec</code></td>
  <td>gzip</td>
  <td>
    Sets the compression codec use when writing Parquet files. Acceptable values include:
    uncompressed, snappy, gzip, lzo.
  </td>
</tr>
<tr>
  <td><code>spark.sql.parquet.filterPushdown</code></td>
  <td>true</td>
  <td>Enables Parquet filter push-down optimization when set to true.</td>
</tr>
<tr>
  <td><code>spark.sql.hive.convertMetastoreParquet</code></td>
  <td>true</td>
  <td>
    When set to false, Spark SQL will use the Hive SerDe for parquet tables instead of the built in
    support.
  </td>
</tr>
<tr>
  <td><code>spark.sql.parquet.mergeSchema</code></td>
  <td><code>false</code></td>
  <td>
    <p>
      When true, the Parquet data source merges schemas collected from all data files, otherwise the
      schema is picked from the summary file or a random data file if no summary file is available.
    </p>
  </td>
</tr>
</table>

## JSON Datasets
<div class="codetabs">

<div data-lang="scala"  markdown="1">
Spark SQL can automatically infer the schema of a JSON dataset and load it as a DataFrame.
This conversion can be done using `SQLContext.read.json()` on either an RDD of String,
or a JSON file.

Note that the file that is offered as _a json file_ is not a typical JSON file. Each
line must contain a separate, self-contained valid JSON object. As a consequence,
a regular multi-line JSON file will most often fail.

{% highlight scala %}
// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// A JSON dataset is pointed to by path.
// The path can be either a single text file or a directory storing text files.
val path = "examples/src/main/resources/people.json"
val people = sqlContext.read.json(path)

// The inferred schema can be visualized using the printSchema() method.
people.printSchema()
// root
//  |-- age: long (nullable = true)
//  |-- name: string (nullable = true)

// Creates a temporary view using the DataFrame
people.createOrReplaceTempView("people")

// SQL statements can be run by using the sql methods provided by sqlContext.
val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

// Alternatively, a DataFrame can be created for a JSON dataset represented by
// an RDD[String] storing one JSON object per string.
val anotherPeopleRDD = sc.parallelize(
  """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
val anotherPeople = sqlContext.read.json(anotherPeopleRDD)
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">
Spark SQL can automatically infer the schema of a JSON dataset and load it as a DataFrame.
This conversion can be done using `SQLContext.read().json()` on either an RDD of String,
or a JSON file.

Note that the file that is offered as _a json file_ is not a typical JSON file. Each
line must contain a separate, self-contained valid JSON object. As a consequence,
a regular multi-line JSON file will most often fail.

{% highlight java %}
// sc is an existing JavaSparkContext.
SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

// A JSON dataset is pointed to by path.
// The path can be either a single text file or a directory storing text files.
DataFrame people = sqlContext.read().json("examples/src/main/resources/people.json");

// The inferred schema can be visualized using the printSchema() method.
people.printSchema();
// root
//  |-- age: long (nullable = true)
//  |-- name: string (nullable = true)

// Creates a temporary view using the DataFrame
people.createOrReplaceTempView("people");

// SQL statements can be run by using the sql methods provided by sqlContext.
DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

// Alternatively, a DataFrame can be created for a JSON dataset represented by
// an RDD[String] storing one JSON object per string.
List<String> jsonData = Arrays.asList(
  "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
JavaRDD<String> anotherPeopleRDD = sc.parallelize(jsonData);
DataFrame anotherPeople = sqlContext.read().json(anotherPeopleRDD);
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
Spark SQL can automatically infer the schema of a JSON dataset and load it as a DataFrame.
This conversion can be done using `SQLContext.read.json` on a JSON file.

Note that the file that is offered as _a json file_ is not a typical JSON file. Each
line must contain a separate, self-contained valid JSON object. As a consequence,
a regular multi-line JSON file will most often fail.

{% highlight python %}
# sc is an existing SparkContext.
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files.
people = sqlContext.read.json("examples/src/main/resources/people.json")

# The inferred schema can be visualized using the printSchema() method.
people.printSchema()
# root
#  |-- age: long (nullable = true)
#  |-- name: string (nullable = true)

# Creates a temporary view using the DataFrame.
people.createOrReplaceTempView("people")

# SQL statements can be run by using the sql methods provided by `sqlContext`.
teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# Alternatively, a DataFrame can be created for a JSON dataset represented by
# an RDD[String] storing one JSON object per string.
anotherPeopleRDD = sc.parallelize([
  '{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}'])
anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)
{% endhighlight %}
</div>

<div data-lang="r"  markdown="1">
Spark SQL can automatically infer the schema of a JSON dataset and load it as a DataFrame. using
the `jsonFile` function, which loads data from a directory of JSON files where each line of the
files is a JSON object.

Note that the file that is offered as _a json file_ is not a typical JSON file. Each
line must contain a separate, self-contained valid JSON object. As a consequence,
a regular multi-line JSON file will most often fail.

{% highlight r %}
# sc is an existing SparkContext.
sqlContext <- sparkRSQL.init(sc)

# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files.
path <- "examples/src/main/resources/people.json"
# Create a DataFrame from the file(s) pointed to by path
people <- read.json(sqlContext, path)

# The inferred schema can be visualized using the printSchema() method.
printSchema(people)
# root
#  |-- age: long (nullable = true)
#  |-- name: string (nullable = true)

# Register this DataFrame as a table.
registerTempTable(people, "people")

# SQL statements can be run by using the sql methods provided by `sqlContext`.
teenagers <- sql(sqlContext, "SELECT name FROM people WHERE age >= 13 AND age <= 19")
{% endhighlight %}
</div>

<div data-lang="sql"  markdown="1">

{% highlight sql %}

CREATE TEMPORARY TABLE jsonTable
USING org.apache.spark.sql.json
OPTIONS (
  path "examples/src/main/resources/people.json"
)

SELECT * FROM jsonTable

{% endhighlight %}

</div>

</div>

## Hive Tables

Spark SQL also supports reading and writing data stored in [Apache Hive](http://hive.apache.org/).
However, since Hive has a large number of dependencies, it is not included in the default Spark assembly.
Hive support is enabled by adding the `-Phive` and `-Phive-thriftserver` flags to Spark's build.
This command builds a new assembly directory that includes Hive. Note that this Hive assembly directory must also be present
on all of the worker nodes, as they will need access to the Hive serialization and deserialization libraries
(SerDes) in order to access data stored in Hive.

Configuration of Hive is done by placing your `hive-site.xml`, `core-site.xml` (for security configuration),
`hdfs-site.xml` (for HDFS configuration) file in `conf/`.

<div class="codetabs">

<div data-lang="scala"  markdown="1">

When working with Hive, one must instantiate `SparkSession` with Hive support, including
connectivity to a persistent Hive metastore, support for Hive serdes, and Hive user-defined functions.
Users who do not have an existing Hive deployment can still enable Hive support. When not configured
by the `hive-site.xml`, the context automatically creates `metastore_db` in the current directory and
creates a directory configured by `spark.sql.warehouse.dir`, which defaults to the directory
`spark-warehouse` in the current directory that the spark application is started. Note that 
the `hive.metastore.warehouse.dir` property in `hive-site.xml` is deprecated since Spark 2.0.0.
Instead, use `spark.sql.warehouse.dir` to specify the default location of database in warehouse.
You may need to grant write privilege to the user who starts the spark application.

{% highlight scala %}
// warehouse_location points to the default location for managed databases and tables
val conf = new SparkConf().setAppName("HiveFromSpark").set("spark.sql.warehouse.dir", warehouse_location)
val spark = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()

spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

// Queries are expressed in HiveQL
spark.sql("FROM src SELECT key, value").collect().foreach(println)
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

When working with Hive, one must instantiate `SparkSession` with Hive support, including
connectivity to a persistent Hive metastore, support for Hive serdes, and Hive user-defined functions.
Users who do not have an existing Hive deployment can still enable Hive support. When not configured
by the `hive-site.xml`, the context automatically creates `metastore_db` in the current directory and
creates a directory configured by `spark.sql.warehouse.dir`, which defaults to the directory
`spark-warehouse` in the current directory that the spark application is started. Note that 
the `hive.metastore.warehouse.dir` property in `hive-site.xml` is deprecated since Spark 2.0.0.
Instead, use `spark.sql.warehouse.dir` to specify the default location of database in warehouse.
You may need to grant write privilege to the user who starts the spark application.

{% highlight java %}
SparkSession spark = SparkSession.builder().appName("JavaSparkSQL").getOrCreate();

spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src");

// Queries are expressed in HiveQL.
Row[] results = spark.sql("FROM src SELECT key, value").collect();

{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

When working with Hive, one must instantiate `SparkSession` with Hive support, including
connectivity to a persistent Hive metastore, support for Hive serdes, and Hive user-defined functions.
Users who do not have an existing Hive deployment can still enable Hive support. When not configured
by the `hive-site.xml`, the context automatically creates `metastore_db` in the current directory and
creates a directory configured by `spark.sql.warehouse.dir`, which defaults to the directory
`spark-warehouse` in the current directory that the spark application is started. Note that 
the `hive.metastore.warehouse.dir` property in `hive-site.xml` is deprecated since Spark 2.0.0.
Instead, use `spark.sql.warehouse.dir` to specify the default location of database in warehouse.
You may need to grant write privilege to the user who starts the spark application.

{% highlight python %}
from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

# Queries can be expressed in HiveQL.
results = spark.sql("FROM src SELECT key, value").collect()

{% endhighlight %}

</div>

<div data-lang="r"  markdown="1">

When working with Hive one must construct a `HiveContext`, which inherits from `SQLContext`, and
adds support for finding tables in the MetaStore and writing queries using HiveQL.
{% highlight r %}
# sc is an existing SparkContext.
sqlContext <- sparkRHive.init(sc)

sql(sqlContext, "CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
sql(sqlContext, "LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

# Queries can be expressed in HiveQL.
results <- collect(sql(sqlContext, "FROM src SELECT key, value"))

{% endhighlight %}

</div>
</div>

### Interacting with Different Versions of Hive Metastore

One of the most important pieces of Spark SQL's Hive support is interaction with Hive metastore,
which enables Spark SQL to access metadata of Hive tables. Starting from Spark 1.4.0, a single binary
build of Spark SQL can be used to query different versions of Hive metastores, using the configuration described below.
Note that independent of the version of Hive that is being used to talk to the metastore, internally Spark SQL
will compile against Hive 1.2.1 and use those classes for internal execution (serdes, UDFs, UDAFs, etc).

The following options can be used to configure the version of Hive that is used to retrieve metadata:

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
  <tr>
    <td><code>spark.sql.hive.metastore.version</code></td>
    <td><code>1.2.1</code></td>
    <td>
      Version of the Hive metastore. Available
      options are <code>0.12.0</code> through <code>1.2.1</code>.
    </td>
  </tr>
  <tr>
    <td><code>spark.sql.hive.metastore.jars</code></td>
    <td><code>builtin</code></td>
    <td>
      Location of the jars that should be used to instantiate the HiveMetastoreClient. This
      property can be one of three options:
      <ol>
        <li><code>builtin</code></li>
        Use Hive 1.2.1, which is bundled with the Spark assembly when <code>-Phive</code> is
        enabled. When this option is chosen, <code>spark.sql.hive.metastore.version</code> must be
        either <code>1.2.1</code> or not defined.
        <li><code>maven</code></li>
        Use Hive jars of specified version downloaded from Maven repositories. This configuration
        is not generally recommended for production deployments.
        <li>A classpath in the standard format for the JVM. This classpath must include all of Hive
        and its dependencies, including the correct version of Hadoop. These jars only need to be
        present on the driver, but if you are running in yarn cluster mode then you must ensure
        they are packaged with you application.</li>
      </ol>
    </td>
  </tr>
  <tr>
    <td><code>spark.sql.hive.metastore.sharedPrefixes</code></td>
    <td><code>com.mysql.jdbc,<br/>org.postgresql,<br/>com.microsoft.sqlserver,<br/>oracle.jdbc</code></td>
    <td>
      <p>
        A comma separated list of class prefixes that should be loaded using the classloader that is
        shared between Spark SQL and a specific version of Hive. An example of classes that should
        be shared is JDBC drivers that are needed to talk to the metastore. Other classes that need
        to be shared are those that interact with classes that are already shared. For example,
        custom appenders that are used by log4j.
      </p>
    </td>
  </tr>
  <tr>
    <td><code>spark.sql.hive.metastore.barrierPrefixes</code></td>
    <td><code>(empty)</code></td>
    <td>
      <p>
        A comma separated list of class prefixes that should explicitly be reloaded for each version
        of Hive that Spark SQL is communicating with. For example, Hive UDFs that are declared in a
        prefix that typically would be shared (i.e. <code>org.apache.spark.*</code>).
      </p>
    </td>
  </tr>
</table>


## JDBC To Other Databases

Spark SQL also includes a data source that can read data from other databases using JDBC. This
functionality should be preferred over using [JdbcRDD](api/scala/index.html#org.apache.spark.rdd.JdbcRDD).
This is because the results are returned
as a DataFrame and they can easily be processed in Spark SQL or joined with other data sources.
The JDBC data source is also easier to use from Java or Python as it does not require the user to
provide a ClassTag.
(Note that this is different than the Spark SQL JDBC server, which allows other applications to
run queries using Spark SQL).

To get started you will need to include the JDBC driver for you particular database on the
spark classpath. For example, to connect to postgres from the Spark Shell you would run the
following command:

{% highlight bash %}
bin/spark-shell --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar
{% endhighlight %}

Tables from the remote database can be loaded as a DataFrame or Spark SQL Temporary table using
the Data Sources API. The following options are supported:

<table class="table">
  <tr><th>Property Name</th><th>Meaning</th></tr>
  <tr>
    <td><code>url</code></td>
    <td>
      The JDBC URL to connect to.
    </td>
  </tr>
  <tr>
    <td><code>dbtable</code></td>
    <td>
      The JDBC table that should be read. Note that anything that is valid in a <code>FROM</code> clause of
      a SQL query can be used. For example, instead of a full table you could also use a
      subquery in parentheses.
    </td>
  </tr>

  <tr>
    <td><code>driver</code></td>
    <td>
      The class name of the JDBC driver to use to connect to this URL.
    </td>
  </tr>
  
  <tr>
    <td><code>partitionColumn, lowerBound, upperBound, numPartitions</code></td>
    <td>
      These options must all be specified if any of them is specified. They describe how to
      partition the table when reading in parallel from multiple workers.
      <code>partitionColumn</code> must be a numeric column from the table in question. Notice
      that <code>lowerBound</code> and <code>upperBound</code> are just used to decide the
      partition stride, not for filtering the rows in table. So all rows in the table will be
      partitioned and returned.
    </td>
  </tr>
  
  <tr>
    <td><code>fetchSize</code></td>
    <td>
      The JDBC fetch size, which determines how many rows to fetch per round trip. This can help performance on JDBC drivers which default to low fetch size (eg. Oracle with 10 rows).
    </td>
  </tr>
</table>

<div class="codetabs">

<div data-lang="scala"  markdown="1">

{% highlight scala %}
val jdbcDF = sqlContext.read.format("jdbc").options(
  Map("url" -> "jdbc:postgresql:dbserver",
  "dbtable" -> "schema.tablename")).load()
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}

Map<String, String> options = new HashMap<>();
options.put("url", "jdbc:postgresql:dbserver");
options.put("dbtable", "schema.tablename");

DataFrame jdbcDF = sqlContext.read().format("jdbc"). options(options).load();
{% endhighlight %}


</div>

<div data-lang="python"  markdown="1">

{% highlight python %}

df = sqlContext.read.format('jdbc').options(url='jdbc:postgresql:dbserver', dbtable='schema.tablename').load()

{% endhighlight %}

</div>

<div data-lang="r"  markdown="1">

{% highlight r %}

df <- loadDF(sqlContext, source="jdbc", url="jdbc:postgresql:dbserver", dbtable="schema.tablename")

{% endhighlight %}

</div>

<div data-lang="sql"  markdown="1">

{% highlight sql %}

CREATE TEMPORARY TABLE jdbcTable
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:postgresql:dbserver",
  dbtable "schema.tablename"
)

{% endhighlight %}

</div>
</div>

## Troubleshooting

 * The JDBC driver class must be visible to the primordial class loader on the client session and on all executors. This is because Java's DriverManager class does a security check that results in it ignoring all drivers not visible to the primordial class loader when one goes to open a connection. One convenient way to do this is to modify compute_classpath.sh on all worker nodes to include your driver JARs.
 * Some databases, such as H2, convert all names to upper case. You'll need to use upper case to refer to those names in Spark SQL.


# Performance Tuning

For some workloads it is possible to improve performance by either caching data in memory, or by
turning on some experimental options.

## Caching Data In Memory

Spark SQL can cache tables using an in-memory columnar format by calling `sqlContext.cacheTable("tableName")` or `dataFrame.cache()`.
Then Spark SQL will scan only required columns and will automatically tune compression to minimize
memory usage and GC pressure. You can call `sqlContext.uncacheTable("tableName")` to remove the table from memory.

Configuration of in-memory caching can be done using the `setConf` method on `SQLContext` or by running
`SET key=value` commands using SQL.

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.sql.inMemoryColumnarStorage.compressed</code></td>
  <td>true</td>
  <td>
    When set to true Spark SQL will automatically select a compression codec for each column based
    on statistics of the data.
  </td>
</tr>
<tr>
  <td><code>spark.sql.inMemoryColumnarStorage.batchSize</code></td>
  <td>10000</td>
  <td>
    Controls the size of batches for columnar caching. Larger batch sizes can improve memory utilization
    and compression, but risk OOMs when caching data.
  </td>
</tr>

</table>

## Other Configuration Options

The following options can also be used to tune the performance of query execution. It is possible
that these options will be deprecated in future release as more optimizations are performed automatically.

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
  <tr>
    <td><code>spark.sql.autoBroadcastJoinThreshold</code></td>
    <td>10485760 (10 MB)</td>
    <td>
      Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when
      performing a join. By setting this value to -1 broadcasting can be disabled. Note that currently
      statistics are only supported for Hive Metastore tables where the command
      <code>ANALYZE TABLE &lt;tableName&gt; COMPUTE STATISTICS noscan</code> has been run.
    </td>
  </tr>
  <tr>
    <td><code>spark.sql.tungsten.enabled</code></td>
    <td>true</td>
    <td>
      When true, use the optimized Tungsten physical execution backend which explicitly manages memory
      and dynamically generates bytecode for expression evaluation.
    </td>
  </tr>
  <tr>
    <td><code>spark.sql.shuffle.partitions</code></td>
    <td>200</td>
    <td>
      Configures the number of partitions to use when shuffling data for joins or aggregations.
    </td>
  </tr>
</table>

# Distributed SQL Engine

Spark SQL can also act as a distributed query engine using its JDBC/ODBC or command-line interface.
In this mode, end-users or applications can interact with Spark SQL directly to run SQL queries,
without the need to write any code.

## Running the Thrift JDBC/ODBC server

The Thrift JDBC/ODBC server implemented here corresponds to the [`HiveServer2`](https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2)
in Hive 1.2.1 You can test the JDBC server with the beeline script that comes with either Spark or Hive 1.2.1.

To start the JDBC/ODBC server, run the following in the Spark directory:

    ./sbin/start-thriftserver.sh

This script accepts all `bin/spark-submit` command line options, plus a `--hiveconf` option to
specify Hive properties. You may run `./sbin/start-thriftserver.sh --help` for a complete list of
all available options. By default, the server listens on localhost:10000. You may override this
behaviour via either environment variables, i.e.:

{% highlight bash %}
export HIVE_SERVER2_THRIFT_PORT=<listening-port>
export HIVE_SERVER2_THRIFT_BIND_HOST=<listening-host>
./sbin/start-thriftserver.sh \
  --master <master-uri> \
  ...
{% endhighlight %}

or system properties:

{% highlight bash %}
./sbin/start-thriftserver.sh \
  --hiveconf hive.server2.thrift.port=<listening-port> \
  --hiveconf hive.server2.thrift.bind.host=<listening-host> \
  --master <master-uri>
  ...
{% endhighlight %}

Now you can use beeline to test the Thrift JDBC/ODBC server:

    ./bin/beeline

Connect to the JDBC/ODBC server in beeline with:

    beeline> !connect jdbc:hive2://localhost:10000

Beeline will ask you for a username and password. In non-secure mode, simply enter the username on
your machine and a blank password. For secure mode, please follow the instructions given in the
[beeline documentation](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients).

Configuration of Hive is done by placing your `hive-site.xml`, `core-site.xml` and `hdfs-site.xml` files in `conf/`.

You may also use the beeline script that comes with Hive.

Thrift JDBC server also supports sending thrift RPC messages over HTTP transport.
Use the following setting to enable HTTP mode as system property or in `hive-site.xml` file in `conf/`:

    hive.server2.transport.mode - Set this to value: http
    hive.server2.thrift.http.port - HTTP port number fo listen on; default is 10001
    hive.server2.http.endpoint - HTTP endpoint; default is cliservice

To test, use beeline to connect to the JDBC/ODBC server in http mode with:

    beeline> !connect jdbc:hive2://<host>:<port>/<database>?hive.server2.transport.mode=http;hive.server2.thrift.http.path=<http_endpoint>


## Running the Spark SQL CLI

The Spark SQL CLI is a convenient tool to run the Hive metastore service in local mode and execute
queries input from the command line. Note that the Spark SQL CLI cannot talk to the Thrift JDBC server.

To start the Spark SQL CLI, run the following in the Spark directory:

    ./bin/spark-sql

Configuration of Hive is done by placing your `hive-site.xml`, `core-site.xml` and `hdfs-site.xml` files in `conf/`.
You may run `./bin/spark-sql --help` for a complete list of all available
options.

# Migration Guide

## Upgrading From Spark SQL 1.5 to 1.6

 - From Spark 1.6, by default the Thrift server runs in multi-session mode. Which means each JDBC/ODBC
   connection owns a copy of their own SQL configuration and temporary function registry. Cached
   tables are still shared though. If you prefer to run the Thrift server in the old single-session
   mode, please set option `spark.sql.hive.thriftServer.singleSession` to `true`. You may either add
   this option to `spark-defaults.conf`, or pass it to `start-thriftserver.sh` via `--conf`:

   {% highlight bash %}
   ./sbin/start-thriftserver.sh \
     --conf spark.sql.hive.thriftServer.singleSession=true \
     ...
   {% endhighlight %}
 - Since 1.6.1, withColumn method in sparkR supports adding a new column to or replacing existing columns
   of the same name of a DataFrame.

 - From Spark 1.6, LongType casts to TimestampType expect seconds instead of microseconds. This
   change was made to match the behavior of Hive 1.2 for more consistent type casting to TimestampType
   from numeric types. See [SPARK-11724](https://issues.apache.org/jira/browse/SPARK-11724) for
   details.

## Upgrading From Spark SQL 1.4 to 1.5

 - Optimized execution using manually managed memory (Tungsten) is now enabled by default, along with
   code generation for expression evaluation. These features can both be disabled by setting
   `spark.sql.tungsten.enabled` to `false`.
 - Parquet schema merging is no longer enabled by default. It can be re-enabled by setting
   `spark.sql.parquet.mergeSchema` to `true`.
 - Resolution of strings to columns in python now supports using dots (`.`) to qualify the column or
   access nested values. For example `df['table.column.nestedField']`. However, this means that if
   your column name contains any dots you must now escape them using backticks (e.g., ``table.`column.with.dots`.nested``).  
 - In-memory columnar storage partition pruning is on by default. It can be disabled by setting
   `spark.sql.inMemoryColumnarStorage.partitionPruning` to `false`.
 - Unlimited precision decimal columns are no longer supported, instead Spark SQL enforces a maximum
   precision of 38. When inferring schema from `BigDecimal` objects, a precision of (38, 18) is now
   used. When no precision is specified in DDL then the default remains `Decimal(10, 0)`.
 - Timestamps are now stored at a precision of 1us, rather than 1ns
 - In the `sql` dialect, floating point numbers are now parsed as decimal. HiveQL parsing remains
   unchanged.
 - The canonical name of SQL/DataFrame functions are now lower case (e.g. sum vs SUM).
 - JSON data source will not automatically load new files that are created by other applications
   (i.e. files that are not inserted to the dataset through Spark SQL).
   For a JSON persistent table (i.e. the metadata of the table is stored in Hive Metastore),
   users can use `REFRESH TABLE` SQL command or `HiveContext`'s `refreshTable` method
   to include those new files to the table. For a DataFrame representing a JSON dataset, users need to recreate
   the DataFrame and the new DataFrame will include new files.
 - DataFrame.withColumn method in pySpark supports adding a new column or replacing existing columns of the same name.

## Upgrading from Spark SQL 1.3 to 1.4

#### DataFrame data reader/writer interface

Based on user feedback, we created a new, more fluid API for reading data in (`SQLContext.read`)
and writing data out (`DataFrame.write`),
and deprecated the old APIs (e.g. `SQLContext.parquetFile`, `SQLContext.jsonFile`).

See the API docs for `SQLContext.read` (
  <a href="api/scala/index.html#org.apache.spark.sql.SQLContext@read:DataFrameReader">Scala</a>,
  <a href="api/java/org/apache/spark/sql/SQLContext.html#read()">Java</a>,
  <a href="api/python/pyspark.sql.html#pyspark.sql.SQLContext.read">Python</a>
) and `DataFrame.write` (
  <a href="api/scala/index.html#org.apache.spark.sql.DataFrame@write:DataFrameWriter">Scala</a>,
  <a href="api/java/org/apache/spark/sql/DataFrame.html#write()">Java</a>,
  <a href="api/python/pyspark.sql.html#pyspark.sql.DataFrame.write">Python</a>
) more information.


#### DataFrame.groupBy retains grouping columns

Based on user feedback, we changed the default behavior of `DataFrame.groupBy().agg()` to retain the
grouping columns in the resulting `DataFrame`. To keep the behavior in 1.3, set `spark.sql.retainGroupColumns` to `false`.

<div class="codetabs">
<div data-lang="scala"  markdown="1">
{% highlight scala %}

// In 1.3.x, in order for the grouping column "department" to show up,
// it must be included explicitly as part of the agg function call.
df.groupBy("department").agg($"department", max("age"), sum("expense"))

// In 1.4+, grouping column "department" is included automatically.
df.groupBy("department").agg(max("age"), sum("expense"))

// Revert to 1.3 behavior (not retaining grouping column) by:
sqlContext.setConf("spark.sql.retainGroupColumns", "false")

{% endhighlight %}
</div>

<div data-lang="java"  markdown="1">
{% highlight java %}

// In 1.3.x, in order for the grouping column "department" to show up,
// it must be included explicitly as part of the agg function call.
df.groupBy("department").agg(col("department"), max("age"), sum("expense"));

// In 1.4+, grouping column "department" is included automatically.
df.groupBy("department").agg(max("age"), sum("expense"));

// Revert to 1.3 behavior (not retaining grouping column) by:
sqlContext.setConf("spark.sql.retainGroupColumns", "false");

{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}

import pyspark.sql.functions as func

# In 1.3.x, in order for the grouping column "department" to show up,
# it must be included explicitly as part of the agg function call.
df.groupBy("department").agg("department"), func.max("age"), func.sum("expense"))

# In 1.4+, grouping column "department" is included automatically.
df.groupBy("department").agg(func.max("age"), func.sum("expense"))

# Revert to 1.3.x behavior (not retaining grouping column) by:
sqlContext.setConf("spark.sql.retainGroupColumns", "false")

{% endhighlight %}
</div>

</div>


#### Behavior change on DataFrame.withColumn

Prior to 1.4, DataFrame.withColumn() supports adding a column only. The column will always be added
as a new column with its specified name in the result DataFrame even if there may be any existing
columns of the same name. Since 1.4, DataFrame.withColumn() supports adding a column of a different
name from names of all existing columns or replacing existing columns of the same name.

Note that this change is only for Scala API, not for PySpark and SparkR.


## Upgrading from Spark SQL 1.0-1.2 to 1.3

In Spark 1.3 we removed the "Alpha" label from Spark SQL and as part of this did a cleanup of the
available APIs. From Spark 1.3 onwards, Spark SQL will provide binary compatibility with other
releases in the 1.X series. This compatibility guarantee excludes APIs that are explicitly marked
as unstable (i.e., DeveloperAPI or Experimental).

#### Rename of SchemaRDD to DataFrame

The largest change that users will notice when upgrading to Spark SQL 1.3 is that `SchemaRDD` has
been renamed to `DataFrame`. This is primarily because DataFrames no longer inherit from RDD
directly, but instead provide most of the functionality that RDDs provide though their own
implementation. DataFrames can still be converted to RDDs by calling the `.rdd` method.

In Scala there is a type alias from `SchemaRDD` to `DataFrame` to provide source compatibility for
some use cases. It is still recommended that users update their code to use `DataFrame` instead.
Java and Python users will need to update their code.

#### Unification of the Java and Scala APIs

Prior to Spark 1.3 there were separate Java compatible classes (`JavaSQLContext` and `JavaSchemaRDD`)
that mirrored the Scala API. In Spark 1.3 the Java API and Scala API have been unified. Users
of either language should use `SQLContext` and `DataFrame`. In general theses classes try to
use types that are usable from both languages (i.e. `Array` instead of language specific collections).
In some cases where no common type exists (e.g., for passing in closures or Maps) function overloading
is used instead.

Additionally the Java specific types API has been removed. Users of both Scala and Java should
use the classes present in `org.apache.spark.sql.types` to describe schema programmatically.


#### Isolation of Implicit Conversions and Removal of dsl Package (Scala-only)

Many of the code examples prior to Spark 1.3 started with `import sqlContext._`, which brought
all of the functions from sqlContext into scope. In Spark 1.3 we have isolated the implicit
conversions for converting `RDD`s into `DataFrame`s into an object inside of the `SQLContext`.
Users should now write `import sqlContext.implicits._`.

Additionally, the implicit conversions now only augment RDDs that are composed of `Product`s (i.e.,
case classes or tuples) with a method `toDF`, instead of applying automatically.

When using function inside of the DSL (now replaced with the `DataFrame` API) users used to import
`org.apache.spark.sql.catalyst.dsl`. Instead the public dataframe functions API should be used:
`import org.apache.spark.sql.functions._`.

#### Removal of the type aliases in org.apache.spark.sql for DataType (Scala-only)

Spark 1.3 removes the type aliases that were present in the base sql package for `DataType`. Users
should instead import the classes in `org.apache.spark.sql.types`

#### UDF Registration Moved to `sqlContext.udf` (Java & Scala)

Functions that are used to register UDFs, either for use in the DataFrame DSL or SQL, have been
moved into the udf object in `SQLContext`.

<div class="codetabs">
<div data-lang="scala"  markdown="1">
{% highlight scala %}

sqlContext.udf.register("strLen", (s: String) => s.length())

{% endhighlight %}
</div>

<div data-lang="java"  markdown="1">
{% highlight java %}

sqlContext.udf().register("strLen", (String s) -> s.length(), DataTypes.IntegerType);

{% endhighlight %}
</div>

</div>

Python UDF registration is unchanged.

#### Python DataTypes No Longer Singletons

When using DataTypes in Python you will need to construct them (i.e. `StringType()`) instead of
referencing a singleton.

## Compatibility with Apache Hive

Spark SQL is designed to be compatible with the Hive Metastore, SerDes and UDFs.
Currently Hive SerDes and UDFs are based on Hive 1.2.1,
and Spark SQL can be connected to different versions of Hive Metastore
(from 0.12.0 to 1.2.1. Also see [Interacting with Different Versions of Hive Metastore] (#interacting-with-different-versions-of-hive-metastore)).

#### Deploying in Existing Hive Warehouses

The Spark SQL Thrift JDBC server is designed to be "out of the box" compatible with existing Hive
installations. You do not need to modify your existing Hive Metastore or change the data placement
or partitioning of your tables.

### Supported Hive Features

Spark SQL supports the vast majority of Hive features, such as:

* Hive query statements, including:
  * `SELECT`
  * `GROUP BY`
  * `ORDER BY`
  * `CLUSTER BY`
  * `SORT BY`
* All Hive operators, including:
  * Relational operators (`=`, `⇔`, `==`, `<>`, `<`, `>`, `>=`, `<=`, etc)
  * Arithmetic operators (`+`, `-`, `*`, `/`, `%`, etc)
  * Logical operators (`AND`, `&&`, `OR`, `||`, etc)
  * Complex type constructors
  * Mathematical functions (`sign`, `ln`, `cos`, etc)
  * String functions (`instr`, `length`, `printf`, etc)
* User defined functions (UDF)
* User defined aggregation functions (UDAF)
* User defined serialization formats (SerDes)
* Window functions
* Joins
  * `JOIN`
  * `{LEFT|RIGHT|FULL} OUTER JOIN`
  * `LEFT SEMI JOIN`
  * `CROSS JOIN`
* Unions
* Sub-queries
  * `SELECT col FROM ( SELECT a + b AS col from t1) t2`
* Sampling
* Explain
* Partitioned tables including dynamic partition insertion
* View
* All Hive DDL Functions, including:
  * `CREATE TABLE`
  * `CREATE TABLE AS SELECT`
  * `ALTER TABLE`
* Most Hive Data types, including:
  * `TINYINT`
  * `SMALLINT`
  * `INT`
  * `BIGINT`
  * `BOOLEAN`
  * `FLOAT`
  * `DOUBLE`
  * `STRING`
  * `BINARY`
  * `TIMESTAMP`
  * `DATE`
  * `ARRAY<>`
  * `MAP<>`
  * `STRUCT<>`

### Unsupported Hive Functionality

Below is a list of Hive features that we don't support yet. Most of these features are rarely used
in Hive deployments.

**Major Hive Features**

* Tables with buckets: bucket is the hash partitioning within a Hive table partition. Spark SQL
  doesn't support buckets yet.


**Esoteric Hive Features**

* `UNION` type
* Unique join
* Column statistics collecting: Spark SQL does not piggyback scans to collect column statistics at
  the moment and only supports populating the sizeInBytes field of the hive metastore.

**Hive Input/Output Formats**

* File format for CLI: For results showing back to the CLI, Spark SQL only supports TextOutputFormat.
* Hadoop archive

**Hive Optimizations**

A handful of Hive optimizations are not yet included in Spark. Some of these (such as indexes) are
less important due to Spark SQL's in-memory computational model. Others are slotted for future
releases of Spark SQL.

* Block level bitmap indexes and virtual columns (used to build indexes)
* Automatically determine the number of reducers for joins and groupbys: Currently in Spark SQL, you
  need to control the degree of parallelism post-shuffle using "`SET spark.sql.shuffle.partitions=[num_tasks];`".
* Meta-data only query: For queries that can be answered by using only meta data, Spark SQL still
  launches tasks to compute the result.
* Skew data flag: Spark SQL does not follow the skew data flags in Hive.
* `STREAMTABLE` hint in join: Spark SQL does not follow the `STREAMTABLE` hint.
* Merge multiple small files for query results: if the result output contains multiple small files,
  Hive can optionally merge the small files into fewer large files to avoid overflowing the HDFS
  metadata. Spark SQL does not support that.

# Reference

## Data Types

Spark SQL and DataFrames support the following data types:

* Numeric types
    - `ByteType`: Represents 1-byte signed integer numbers.
    The range of numbers is from `-128` to `127`.
    - `ShortType`: Represents 2-byte signed integer numbers.
    The range of numbers is from `-32768` to `32767`.
    - `IntegerType`: Represents 4-byte signed integer numbers.
    The range of numbers is from `-2147483648` to `2147483647`.
    - `LongType`: Represents 8-byte signed integer numbers.
    The range of numbers is from `-9223372036854775808` to `9223372036854775807`.
    - `FloatType`: Represents 4-byte single-precision floating point numbers.
    - `DoubleType`: Represents 8-byte double-precision floating point numbers.
    - `DecimalType`: Represents arbitrary-precision signed decimal numbers. Backed internally by `java.math.BigDecimal`. A `BigDecimal` consists of an arbitrary precision integer unscaled value and a 32-bit integer scale.
* String type
    - `StringType`: Represents character string values.
* Binary type
    - `BinaryType`: Represents byte sequence values.
* Boolean type
    - `BooleanType`: Represents boolean values.
* Datetime type
    - `TimestampType`: Represents values comprising values of fields year, month, day,
    hour, minute, and second.
    - `DateType`: Represents values comprising values of fields year, month, day.
* Complex types
    - `ArrayType(elementType, containsNull)`: Represents values comprising a sequence of
    elements with the type of `elementType`. `containsNull` is used to indicate if
    elements in a `ArrayType` value can have `null` values.
    - `MapType(keyType, valueType, valueContainsNull)`:
    Represents values comprising a set of key-value pairs. The data type of keys are
    described by `keyType` and the data type of values are described by `valueType`.
    For a `MapType` value, keys are not allowed to have `null` values. `valueContainsNull`
    is used to indicate if values of a `MapType` value can have `null` values.
    - `StructType(fields)`: Represents values with the structure described by
    a sequence of `StructField`s (`fields`).
        * `StructField(name, dataType, nullable)`: Represents a field in a `StructType`.
        The name of a field is indicated by `name`. The data type of a field is indicated
        by `dataType`. `nullable` is used to indicate if values of this fields can have
        `null` values.

<div class="codetabs">
<div data-lang="scala"  markdown="1">

All data types of Spark SQL are located in the package `org.apache.spark.sql.types`.
You can access them by doing
{% highlight scala %}
import  org.apache.spark.sql.types._
{% endhighlight %}

<table class="table">
<tr>
  <th style="width:20%">Data type</th>
  <th style="width:40%">Value type in Scala</th>
  <th>API to access or create a data type</th></tr>
<tr>
  <td> <b>ByteType</b> </td>
  <td> Byte </td>
  <td>
  ByteType
  </td>
</tr>
<tr>
  <td> <b>ShortType</b> </td>
  <td> Short </td>
  <td>
  ShortType
  </td>
</tr>
<tr>
  <td> <b>IntegerType</b> </td>
  <td> Int </td>
  <td>
  IntegerType
  </td>
</tr>
<tr>
  <td> <b>LongType</b> </td>
  <td> Long </td>
  <td>
  LongType
  </td>
</tr>
<tr>
  <td> <b>FloatType</b> </td>
  <td> Float </td>
  <td>
  FloatType
  </td>
</tr>
<tr>
  <td> <b>DoubleType</b> </td>
  <td> Double </td>
  <td>
  DoubleType
  </td>
</tr>
<tr>
  <td> <b>DecimalType</b> </td>
  <td> java.math.BigDecimal </td>
  <td>
  DecimalType
  </td>
</tr>
<tr>
  <td> <b>StringType</b> </td>
  <td> String </td>
  <td>
  StringType
  </td>
</tr>
<tr>
  <td> <b>BinaryType</b> </td>
  <td> Array[Byte] </td>
  <td>
  BinaryType
  </td>
</tr>
<tr>
  <td> <b>BooleanType</b> </td>
  <td> Boolean </td>
  <td>
  BooleanType
  </td>
</tr>
<tr>
  <td> <b>TimestampType</b> </td>
  <td> java.sql.Timestamp </td>
  <td>
  TimestampType
  </td>
</tr>
<tr>
  <td> <b>DateType</b> </td>
  <td> java.sql.Date </td>
  <td>
  DateType
  </td>
</tr>
<tr>
  <td> <b>ArrayType</b> </td>
  <td> scala.collection.Seq </td>
  <td>
  ArrayType(<i>elementType</i>, [<i>containsNull</i>])<br />
  <b>Note:</b> The default value of <i>containsNull</i> is <i>true</i>.
  </td>
</tr>
<tr>
  <td> <b>MapType</b> </td>
  <td> scala.collection.Map </td>
  <td>
  MapType(<i>keyType</i>, <i>valueType</i>, [<i>valueContainsNull</i>])<br />
  <b>Note:</b> The default value of <i>valueContainsNull</i> is <i>true</i>.
  </td>
</tr>
<tr>
  <td> <b>StructType</b> </td>
  <td> org.apache.spark.sql.Row </td>
  <td>
  StructType(<i>fields</i>)<br />
  <b>Note:</b> <i>fields</i> is a Seq of StructFields. Also, two fields with the same
  name are not allowed.
  </td>
</tr>
<tr>
  <td> <b>StructField</b> </td>
  <td> The value type in Scala of the data type of this field
  (For example, Int for a StructField with the data type IntegerType) </td>
  <td>
  StructField(<i>name</i>, <i>dataType</i>, <i>nullable</i>)
  </td>
</tr>
</table>

</div>

<div data-lang="java" markdown="1">

All data types of Spark SQL are located in the package of
`org.apache.spark.sql.types`. To access or create a data type,
please use factory methods provided in
`org.apache.spark.sql.types.DataTypes`.

<table class="table">
<tr>
  <th style="width:20%">Data type</th>
  <th style="width:40%">Value type in Java</th>
  <th>API to access or create a data type</th></tr>
<tr>
  <td> <b>ByteType</b> </td>
  <td> byte or Byte </td>
  <td>
  DataTypes.ByteType
  </td>
</tr>
<tr>
  <td> <b>ShortType</b> </td>
  <td> short or Short </td>
  <td>
  DataTypes.ShortType
  </td>
</tr>
<tr>
  <td> <b>IntegerType</b> </td>
  <td> int or Integer </td>
  <td>
  DataTypes.IntegerType
  </td>
</tr>
<tr>
  <td> <b>LongType</b> </td>
  <td> long or Long </td>
  <td>
  DataTypes.LongType
  </td>
</tr>
<tr>
  <td> <b>FloatType</b> </td>
  <td> float or Float </td>
  <td>
  DataTypes.FloatType
  </td>
</tr>
<tr>
  <td> <b>DoubleType</b> </td>
  <td> double or Double </td>
  <td>
  DataTypes.DoubleType
  </td>
</tr>
<tr>
  <td> <b>DecimalType</b> </td>
  <td> java.math.BigDecimal </td>
  <td>
  DataTypes.createDecimalType()<br />
  DataTypes.createDecimalType(<i>precision</i>, <i>scale</i>).
  </td>
</tr>
<tr>
  <td> <b>StringType</b> </td>
  <td> String </td>
  <td>
  DataTypes.StringType
  </td>
</tr>
<tr>
  <td> <b>BinaryType</b> </td>
  <td> byte[] </td>
  <td>
  DataTypes.BinaryType
  </td>
</tr>
<tr>
  <td> <b>BooleanType</b> </td>
  <td> boolean or Boolean </td>
  <td>
  DataTypes.BooleanType
  </td>
</tr>
<tr>
  <td> <b>TimestampType</b> </td>
  <td> java.sql.Timestamp </td>
  <td>
  DataTypes.TimestampType
  </td>
</tr>
<tr>
  <td> <b>DateType</b> </td>
  <td> java.sql.Date </td>
  <td>
  DataTypes.DateType
  </td>
</tr>
<tr>
  <td> <b>ArrayType</b> </td>
  <td> java.util.List </td>
  <td>
  DataTypes.createArrayType(<i>elementType</i>)<br />
  <b>Note:</b> The value of <i>containsNull</i> will be <i>true</i><br />
  DataTypes.createArrayType(<i>elementType</i>, <i>containsNull</i>).
  </td>
</tr>
<tr>
  <td> <b>MapType</b> </td>
  <td> java.util.Map </td>
  <td>
  DataTypes.createMapType(<i>keyType</i>, <i>valueType</i>)<br />
  <b>Note:</b> The value of <i>valueContainsNull</i> will be <i>true</i>.<br />
  DataTypes.createMapType(<i>keyType</i>, <i>valueType</i>, <i>valueContainsNull</i>)<br />
  </td>
</tr>
<tr>
  <td> <b>StructType</b> </td>
  <td> org.apache.spark.sql.Row </td>
  <td>
  DataTypes.createStructType(<i>fields</i>)<br />
  <b>Note:</b> <i>fields</i> is a List or an array of StructFields.
  Also, two fields with the same name are not allowed.
  </td>
</tr>
<tr>
  <td> <b>StructField</b> </td>
  <td> The value type in Java of the data type of this field
  (For example, int for a StructField with the data type IntegerType) </td>
  <td>
  DataTypes.createStructField(<i>name</i>, <i>dataType</i>, <i>nullable</i>)
  </td>
</tr>
</table>

</div>

<div data-lang="python"  markdown="1">

All data types of Spark SQL are located in the package of `pyspark.sql.types`.
You can access them by doing
{% highlight python %}
from pyspark.sql.types import *
{% endhighlight %}

<table class="table">
<tr>
  <th style="width:20%">Data type</th>
  <th style="width:40%">Value type in Python</th>
  <th>API to access or create a data type</th></tr>
<tr>
  <td> <b>ByteType</b> </td>
  <td>
  int or long <br />
  <b>Note:</b> Numbers will be converted to 1-byte signed integer numbers at runtime.
  Please make sure that numbers are within the range of -128 to 127.
  </td>
  <td>
  ByteType()
  </td>
</tr>
<tr>
  <td> <b>ShortType</b> </td>
  <td>
  int or long <br />
  <b>Note:</b> Numbers will be converted to 2-byte signed integer numbers at runtime.
  Please make sure that numbers are within the range of -32768 to 32767.
  </td>
  <td>
  ShortType()
  </td>
</tr>
<tr>
  <td> <b>IntegerType</b> </td>
  <td> int or long </td>
  <td>
  IntegerType()
  </td>
</tr>
<tr>
  <td> <b>LongType</b> </td>
  <td>
  long <br />
  <b>Note:</b> Numbers will be converted to 8-byte signed integer numbers at runtime.
  Please make sure that numbers are within the range of
  -9223372036854775808 to 9223372036854775807.
  Otherwise, please convert data to decimal.Decimal and use DecimalType.
  </td>
  <td>
  LongType()
  </td>
</tr>
<tr>
  <td> <b>FloatType</b> </td>
  <td>
  float <br />
  <b>Note:</b> Numbers will be converted to 4-byte single-precision floating
  point numbers at runtime.
  </td>
  <td>
  FloatType()
  </td>
</tr>
<tr>
  <td> <b>DoubleType</b> </td>
  <td> float </td>
  <td>
  DoubleType()
  </td>
</tr>
<tr>
  <td> <b>DecimalType</b> </td>
  <td> decimal.Decimal </td>
  <td>
  DecimalType()
  </td>
</tr>
<tr>
  <td> <b>StringType</b> </td>
  <td> string </td>
  <td>
  StringType()
  </td>
</tr>
<tr>
  <td> <b>BinaryType</b> </td>
  <td> bytearray </td>
  <td>
  BinaryType()
  </td>
</tr>
<tr>
  <td> <b>BooleanType</b> </td>
  <td> bool </td>
  <td>
  BooleanType()
  </td>
</tr>
<tr>
  <td> <b>TimestampType</b> </td>
  <td> datetime.datetime </td>
  <td>
  TimestampType()
  </td>
</tr>
<tr>
  <td> <b>DateType</b> </td>
  <td> datetime.date </td>
  <td>
  DateType()
  </td>
</tr>
<tr>
  <td> <b>ArrayType</b> </td>
  <td> list, tuple, or array </td>
  <td>
  ArrayType(<i>elementType</i>, [<i>containsNull</i>])<br />
  <b>Note:</b> The default value of <i>containsNull</i> is <i>True</i>.
  </td>
</tr>
<tr>
  <td> <b>MapType</b> </td>
  <td> dict </td>
  <td>
  MapType(<i>keyType</i>, <i>valueType</i>, [<i>valueContainsNull</i>])<br />
  <b>Note:</b> The default value of <i>valueContainsNull</i> is <i>True</i>.
  </td>
</tr>
<tr>
  <td> <b>StructType</b> </td>
  <td> list or tuple </td>
  <td>
  StructType(<i>fields</i>)<br />
  <b>Note:</b> <i>fields</i> is a Seq of StructFields. Also, two fields with the same
  name are not allowed.
  </td>
</tr>
<tr>
  <td> <b>StructField</b> </td>
  <td> The value type in Python of the data type of this field
  (For example, Int for a StructField with the data type IntegerType) </td>
  <td>
  StructField(<i>name</i>, <i>dataType</i>, <i>nullable</i>)
  </td>
</tr>
</table>

</div>

<div data-lang="r"  markdown="1">

<table class="table">
<tr>
  <th style="width:20%">Data type</th>
  <th style="width:40%">Value type in R</th>
  <th>API to access or create a data type</th></tr>
<tr>
  <td> <b>ByteType</b> </td>
  <td>
  integer <br />
  <b>Note:</b> Numbers will be converted to 1-byte signed integer numbers at runtime.
  Please make sure that numbers are within the range of -128 to 127.
  </td>
  <td>
  "byte"
  </td>
</tr>
<tr>
  <td> <b>ShortType</b> </td>
  <td>
  integer <br />
  <b>Note:</b> Numbers will be converted to 2-byte signed integer numbers at runtime.
  Please make sure that numbers are within the range of -32768 to 32767.
  </td>
  <td>
  "short"
  </td>
</tr>
<tr>
  <td> <b>IntegerType</b> </td>
  <td> integer </td>
  <td>
  "integer"
  </td>
</tr>
<tr>
  <td> <b>LongType</b> </td>
  <td>
  integer <br />
  <b>Note:</b> Numbers will be converted to 8-byte signed integer numbers at runtime.
  Please make sure that numbers are within the range of
  -9223372036854775808 to 9223372036854775807.
  Otherwise, please convert data to decimal.Decimal and use DecimalType.
  </td>
  <td>
  "long"
  </td>
</tr>
<tr>
  <td> <b>FloatType</b> </td>
  <td>
  numeric <br />
  <b>Note:</b> Numbers will be converted to 4-byte single-precision floating
  point numbers at runtime.
  </td>
  <td>
  "float"
  </td>
</tr>
<tr>
  <td> <b>DoubleType</b> </td>
  <td> numeric </td>
  <td>
  "double"
  </td>
</tr>
<tr>
  <td> <b>DecimalType</b> </td>
  <td> Not supported </td>
  <td>
   Not supported
  </td>
</tr>
<tr>
  <td> <b>StringType</b> </td>
  <td> character </td>
  <td>
  "string"
  </td>
</tr>
<tr>
  <td> <b>BinaryType</b> </td>
  <td> raw </td>
  <td>
  "binary"
  </td>
</tr>
<tr>
  <td> <b>BooleanType</b> </td>
  <td> logical </td>
  <td>
  "bool"
  </td>
</tr>
<tr>
  <td> <b>TimestampType</b> </td>
  <td> POSIXct </td>
  <td>
  "timestamp"
  </td>
</tr>
<tr>
  <td> <b>DateType</b> </td>
  <td> Date </td>
  <td>
  "date"
  </td>
</tr>
<tr>
  <td> <b>ArrayType</b> </td>
  <td> vector or list </td>
  <td>
  list(type="array", elementType=<i>elementType</i>, containsNull=[<i>containsNull</i>])<br />
  <b>Note:</b> The default value of <i>containsNull</i> is <i>True</i>.
  </td>
</tr>
<tr>
  <td> <b>MapType</b> </td>
  <td> environment </td>
  <td>
  list(type="map", keyType=<i>keyType</i>, valueType=<i>valueType</i>, valueContainsNull=[<i>valueContainsNull</i>])<br />
  <b>Note:</b> The default value of <i>valueContainsNull</i> is <i>True</i>.
  </td>
</tr>
<tr>
  <td> <b>StructType</b> </td>
  <td> named list</td>
  <td>
  list(type="struct", fields=<i>fields</i>)<br />
  <b>Note:</b> <i>fields</i> is a Seq of StructFields. Also, two fields with the same
  name are not allowed.
  </td>
</tr>
<tr>
  <td> <b>StructField</b> </td>
  <td> The value type in R of the data type of this field
  (For example, integer for a StructField with the data type IntegerType) </td>
  <td>
  list(name=<i>name</i>, type=<i>dataType</i>, nullable=<i>nullable</i>)
  </td>
</tr>
</table>

</div>

</div>

## NaN Semantics

There is specially handling for not-a-number (NaN) when dealing with `float` or `double` types that
does not exactly match standard floating point semantics.
Specifically:

 - NaN = NaN returns true.
 - In aggregations all NaN values are grouped together.
 - NaN is treated as a normal value in join keys.
 - NaN values go last when in ascending order, larger than any other numeric value.
