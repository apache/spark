---
layout: global
title: Spark SQL Programming Guide
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview

<div class="codetabs">
<div data-lang="scala"  markdown="1">

Spark SQL allows relational queries expressed in SQL, HiveQL, or Scala to be executed using
Spark.  At the core of this component is a new type of RDD,
[SchemaRDD](api/scala/index.html#org.apache.spark.sql.SchemaRDD).  SchemaRDDs are composed of
[Row](api/scala/index.html#org.apache.spark.sql.package@Row:org.apache.spark.sql.catalyst.expressions.Row.type) objects, along with
a schema that describes the data types of each column in the row.  A SchemaRDD is similar to a table
in a traditional relational database.  A SchemaRDD can be created from an existing RDD, a [Parquet](http://parquet.io)
file, a JSON dataset, or by running HiveQL against data stored in [Apache Hive](http://hive.apache.org/).

All of the examples on this page use sample data included in the Spark distribution and can be run in the `spark-shell`.

</div>

<div data-lang="java"  markdown="1">
Spark SQL allows relational queries expressed in SQL or HiveQL to be executed using
Spark.  At the core of this component is a new type of RDD,
[JavaSchemaRDD](api/scala/index.html#org.apache.spark.sql.api.java.JavaSchemaRDD).  JavaSchemaRDDs are composed of
[Row](api/scala/index.html#org.apache.spark.sql.api.java.Row) objects, along with
a schema that describes the data types of each column in the row.  A JavaSchemaRDD is similar to a table
in a traditional relational database.  A JavaSchemaRDD can be created from an existing RDD, a [Parquet](http://parquet.io)
file, a JSON dataset, or by running HiveQL against data stored in [Apache Hive](http://hive.apache.org/).
</div>

<div data-lang="python"  markdown="1">

Spark SQL allows relational queries expressed in SQL or HiveQL to be executed using
Spark.  At the core of this component is a new type of RDD,
[SchemaRDD](api/python/pyspark.sql.SchemaRDD-class.html).  SchemaRDDs are composed of
[Row](api/python/pyspark.sql.Row-class.html) objects, along with
a schema that describes the data types of each column in the row.  A SchemaRDD is similar to a table
in a traditional relational database.  A SchemaRDD can be created from an existing RDD, a [Parquet](http://parquet.io)
file, a JSON dataset, or by running HiveQL against data stored in [Apache Hive](http://hive.apache.org/).

All of the examples on this page use sample data included in the Spark distribution and can be run in the `pyspark` shell.
</div>
</div>

**Spark SQL is currently an alpha component. While we will minimize API changes, some APIs may change in future releases.**

***************************************************************************************************

# Getting Started

<div class="codetabs">
<div data-lang="scala"  markdown="1">

The entry point into all relational functionality in Spark is the
[SQLContext](api/scala/index.html#org.apache.spark.sql.SQLContext) class, or one of its
descendants.  To create a basic SQLContext, all you need is a SparkContext.

{% highlight scala %}
val sc: SparkContext // An existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
import sqlContext.createSchemaRDD
{% endhighlight %}

In addition to the basic SQLContext, you can also create a HiveContext, which provides a
superset of the functionality provided by the basic SQLContext. Additional features include
the ability to write queries using the more complete HiveQL parser, access to HiveUDFs, and the
ability to read data from Hive tables.  To use a HiveContext, you do not need to have an
existing Hive setup, and all of the data sources available to a SQLContext are still available.
HiveContext is only packaged separately to avoid including all of Hive's dependencies in the default
Spark build.  If these dependencies are not a problem for your application then using HiveContext
is recommended for the 1.2 release of Spark.  Future releases will focus on bringing SQLContext up to
feature parity with a HiveContext.

</div>

<div data-lang="java" markdown="1">

The entry point into all relational functionality in Spark is the
[JavaSQLContext](api/scala/index.html#org.apache.spark.sql.api.java.JavaSQLContext) class, or one
of its descendants.  To create a basic JavaSQLContext, all you need is a JavaSparkContext.

{% highlight java %}
JavaSparkContext sc = ...; // An existing JavaSparkContext.
JavaSQLContext sqlContext = new org.apache.spark.sql.api.java.JavaSQLContext(sc);
{% endhighlight %}

In addition to the basic SQLContext, you can also create a HiveContext, which provides a strict
super set of the functionality provided by the basic SQLContext. Additional features include
the ability to write queries using the more complete HiveQL parser, access to HiveUDFs, and the
ability to read data from Hive tables.  To use a HiveContext, you do not need to have an
existing Hive setup, and all of the data sources available to a SQLContext are still available.
HiveContext is only packaged separately to avoid including all of Hive's dependencies in the default
Spark build.  If these dependencies are not a problem for your application then using HiveContext
is recommended for the 1.2 release of Spark.  Future releases will focus on bringing SQLContext up to
feature parity with a HiveContext.

</div>

<div data-lang="python"  markdown="1">

The entry point into all relational functionality in Spark is the
[SQLContext](api/python/pyspark.sql.SQLContext-class.html) class, or one
of its decedents.  To create a basic SQLContext, all you need is a SparkContext.

{% highlight python %}
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
{% endhighlight %}

In addition to the basic SQLContext, you can also create a HiveContext, which provides a strict
super set of the functionality provided by the basic SQLContext. Additional features include
the ability to write queries using the more complete HiveQL parser, access to HiveUDFs, and the
ability to read data from Hive tables.  To use a HiveContext, you do not need to have an
existing Hive setup, and all of the data sources available to a SQLContext are still available.
HiveContext is only packaged separately to avoid including all of Hive's dependencies in the default
Spark build.  If these dependencies are not a problem for your application then using HiveContext
is recommended for the 1.2 release of Spark.  Future releases will focus on bringing SQLContext up to
feature parity with a HiveContext.

</div>

</div>

The specific variant of SQL that is used to parse queries can also be selected using the
`spark.sql.dialect` option.  This parameter can be changed using either the `setConf` method on
a SQLContext or by using a `SET key=value` command in SQL.  For a SQLContext, the only dialect
available is "sql" which uses a simple SQL parser provided by Spark SQL.  In a HiveContext, the
default is "hiveql", though "sql" is also available.  Since the HiveQL parser is much more complete,
 this is recommended for most use cases.

# Data Sources

Spark SQL supports operating on a variety of data sources through the `SchemaRDD` interface.
A SchemaRDD can be operated on as normal RDDs and can also be registered as a temporary table.
Registering a SchemaRDD as a table allows you to run SQL queries over its data.  This section
describes the various methods for loading data into a SchemaRDD.

## RDDs

Spark SQL supports two different methods for converting existing RDDs into SchemaRDDs.  The first
method uses reflection to infer the schema of an RDD that contains specific types of objects.  This
reflection based approach leads to more concise code and works well when you already know the schema 
while writing your Spark application.

The second method for creating SchemaRDDs is through a programmatic interface that allows you to
construct a schema and then apply it to an existing RDD.  While this method is more verbose, it allows
you to construct SchemaRDDs when the columns and their types are not known until runtime.

### Inferring the Schema Using Reflection
<div class="codetabs">

<div data-lang="scala"  markdown="1">

The Scala interaface for Spark SQL supports automatically converting an RDD containing case classes
to a SchemaRDD.  The case class
defines the schema of the table.  The names of the arguments to the case class are read using
reflection and become the names of the columns. Case classes can also be nested or contain complex
types such as Sequences or Arrays. This RDD can be implicitly converted to a SchemaRDD and then be
registered as a table.  Tables can be used in subsequent SQL statements.

{% highlight scala %}
// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
import sqlContext.createSchemaRDD

// Define the schema using a case class.
// Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
// you can use custom classes that implement the Product interface.
case class Person(name: String, age: Int)

// Create an RDD of Person objects and register it as a table.
val people = sc.textFile("examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
people.registerTempTable("people")

// SQL statements can be run by using the sql methods provided by sqlContext.
val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

// The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
// The columns of a row in the result can be accessed by ordinal.
teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

Spark SQL supports automatically converting an RDD of [JavaBeans](http://stackoverflow.com/questions/3295496/what-is-a-javabean-exactly)
into a Schema RDD.  The BeanInfo, obtained using reflection, defines the schema of the table.
Currently, Spark SQL does not support JavaBeans that contain
nested or contain complex types such as Lists or Arrays.  You can create a JavaBean by creating a
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


A schema can be applied to an existing RDD by calling `applySchema` and providing the Class object
for the JavaBean.

{% highlight java %}
// sc is an existing JavaSparkContext.
JavaSQLContext sqlContext = new org.apache.spark.sql.api.java.JavaSQLContext(sc);

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
JavaSchemaRDD schemaPeople = sqlContext.applySchema(people, Person.class);
schemaPeople.registerTempTable("people");

// SQL can be run over RDDs that have been registered as tables.
JavaSchemaRDD teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

// The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
// The columns of a row in the result can be accessed by ordinal.
List<String> teenagerNames = teenagers.map(new Function<Row, String>() {
  public String call(Row row) {
    return "Name: " + row.getString(0);
  }
}).collect();

{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

Spark SQL can convert an RDD of Row objects to a SchemaRDD, inferring the datatypes.  Rows are constructed by passing a list of
key/value pairs as kwargs to the Row class. The keys of this list define the column names of the table,
and the types are inferred by looking at the first row.  Since we currently only look at the first
row, it is important that there is no missing data in the first row of the RDD. In future versions we
plan to more completely infer the schema by looking at more data, similar to the inference that is
performed on JSON files.

{% highlight python %}
# sc is an existing SparkContext.
from pyspark.sql import SQLContext, Row
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a dictionary.
lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the SchemaRDD as a table.
schemaPeople = sqlContext.inferSchema(people)
schemaPeople.registerTempTable("people")

# SQL can be run over SchemaRDDs that have been registered as a table.
teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
teenNames = teenagers.map(lambda p: "Name: " + p.name)
for teenName in teenNames.collect():
  print teenName
{% endhighlight %}

</div>

</div>

### Programmatically Specifying the Schema

<div class="codetabs">

<div data-lang="scala"  markdown="1">

When case classes cannot be defined ahead of time (for example,
the structure of records is encoded in a string, or a text dataset will be parsed
and fields will be projected differently for different users),
a `SchemaRDD` can be created programmatically with three steps.

1. Create an RDD of `Row`s from the original RDD;
2. Create the schema represented by a `StructType` matching the structure of
`Row`s in the RDD created in Step 1.
3. Apply the schema to the RDD of `Row`s via `applySchema` method provided
by `SQLContext`.

For example:
{% highlight scala %}
// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// Create an RDD
val people = sc.textFile("examples/src/main/resources/people.txt")

// The schema is encoded in a string
val schemaString = "name age"

// Import Spark SQL data types and Row.
import org.apache.spark.sql._

// Generate the schema based on the string of schema
val schema =
  StructType(
    schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

// Convert records of the RDD (people) to Rows.
val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

// Apply the schema to the RDD.
val peopleSchemaRDD = sqlContext.applySchema(rowRDD, schema)

// Register the SchemaRDD as a table.
peopleSchemaRDD.registerTempTable("people")

// SQL statements can be run by using the sql methods provided by sqlContext.
val results = sqlContext.sql("SELECT name FROM people")

// The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
// The columns of a row in the result can be accessed by ordinal.
results.map(t => "Name: " + t(0)).collect().foreach(println)
{% endhighlight %}


</div>

<div data-lang="java"  markdown="1">

When JavaBean classes cannot be defined ahead of time (for example,
the structure of records is encoded in a string, or a text dataset will be parsed and
fields will be projected differently for different users),
a `SchemaRDD` can be created programmatically with three steps.

1. Create an RDD of `Row`s from the original RDD;
2. Create the schema represented by a `StructType` matching the structure of
`Row`s in the RDD created in Step 1.
3. Apply the schema to the RDD of `Row`s via `applySchema` method provided
by `JavaSQLContext`.

For example:
{% highlight java %}
// Import factory methods provided by DataType.
import org.apache.spark.sql.api.java.DataType
// Import StructType and StructField
import org.apache.spark.sql.api.java.StructType
import org.apache.spark.sql.api.java.StructField
// Import Row.
import org.apache.spark.sql.api.java.Row

// sc is an existing JavaSparkContext.
JavaSQLContext sqlContext = new org.apache.spark.sql.api.java.JavaSQLContext(sc);

// Load a text file and convert each line to a JavaBean.
JavaRDD<String> people = sc.textFile("examples/src/main/resources/people.txt");

// The schema is encoded in a string
String schemaString = "name age";

// Generate the schema based on the string of schema
List<StructField> fields = new ArrayList<StructField>();
for (String fieldName: schemaString.split(" ")) {
  fields.add(DataType.createStructField(fieldName, DataType.StringType, true));
}
StructType schema = DataType.createStructType(fields);

// Convert records of the RDD (people) to Rows.
JavaRDD<Row> rowRDD = people.map(
  new Function<String, Row>() {
    public Row call(String record) throws Exception {
      String[] fields = record.split(",");
      return Row.create(fields[0], fields[1].trim());
    }
  });

// Apply the schema to the RDD.
JavaSchemaRDD peopleSchemaRDD = sqlContext.applySchema(rowRDD, schema);

// Register the SchemaRDD as a table.
peopleSchemaRDD.registerTempTable("people");

// SQL can be run over RDDs that have been registered as tables.
JavaSchemaRDD results = sqlContext.sql("SELECT name FROM people");

// The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
// The columns of a row in the result can be accessed by ordinal.
List<String> names = results.map(new Function<Row, String>() {
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
a `SchemaRDD` can be created programmatically with three steps.

1. Create an RDD of tuples or lists from the original RDD;
2. Create the schema represented by a `StructType` matching the structure of
tuples or lists in the RDD created in the step 1.
3. Apply the schema to the RDD via `applySchema` method provided by `SQLContext`.

For example:
{% highlight python %}
# Import SQLContext and data types
from pyspark.sql import *

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
schemaPeople = sqlContext.applySchema(people, schema)

# Register the SchemaRDD as a table.
schemaPeople.registerTempTable("people")

# SQL can be run over SchemaRDDs that have been registered as a table.
results = sqlContext.sql("SELECT name FROM people")

# The results of SQL queries are RDDs and support all the normal RDD operations.
names = results.map(lambda p: "Name: " + p.name)
for name in names.collect():
  print name
{% endhighlight %}


</div>

</div>

## Parquet Files

[Parquet](http://parquet.io) is a columnar format that is supported by many other data processing systems.
Spark SQL provides support for both reading and writing Parquet files that automatically preserves the schema
of the original data.

### Loading Data Programmatically

Using the data from the above example:

<div class="codetabs">

<div data-lang="scala"  markdown="1">

{% highlight scala %}
// sqlContext from the previous example is used in this example.
// createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
import sqlContext.createSchemaRDD

val people: RDD[Person] = ... // An RDD of case class objects, from the previous example.

// The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, allowing it to be stored using Parquet.
people.saveAsParquetFile("people.parquet")

// Read in the parquet file created above.  Parquet files are self-describing so the schema is preserved.
// The result of loading a Parquet file is also a SchemaRDD.
val parquetFile = sqlContext.parquetFile("people.parquet")

//Parquet files can also be registered as tables and then used in SQL statements.
parquetFile.registerTempTable("parquetFile")
val teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}
// sqlContext from the previous example is used in this example.

JavaSchemaRDD schemaPeople = ... // The JavaSchemaRDD from the previous example.

// JavaSchemaRDDs can be saved as Parquet files, maintaining the schema information.
schemaPeople.saveAsParquetFile("people.parquet");

// Read in the Parquet file created above.  Parquet files are self-describing so the schema is preserved.
// The result of loading a parquet file is also a JavaSchemaRDD.
JavaSchemaRDD parquetFile = sqlContext.parquetFile("people.parquet");

//Parquet files can also be registered as tables and then used in SQL statements.
parquetFile.registerTempTable("parquetFile");
JavaSchemaRDD teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19");
List<String> teenagerNames = teenagers.map(new Function<Row, String>() {
  public String call(Row row) {
    return "Name: " + row.getString(0);
  }
}).collect();
{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

{% highlight python %}
# sqlContext from the previous example is used in this example.

schemaPeople # The SchemaRDD from the previous example.

# SchemaRDDs can be saved as Parquet files, maintaining the schema information.
schemaPeople.saveAsParquetFile("people.parquet")

# Read in the Parquet file created above.  Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a SchemaRDD.
parquetFile = sqlContext.parquetFile("people.parquet")

# Parquet files can also be registered as tables and then used in SQL statements.
parquetFile.registerTempTable("parquetFile");
teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
teenNames = teenagers.map(lambda p: "Name: " + p.name)
for teenName in teenNames.collect():
  print teenName
{% endhighlight %}

</div>

</div>

### Configuration

Configuration of Parquet can be done using the `setConf` method on SQLContext or by running 
`SET key=value` commands using SQL.

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.sql.parquet.binaryAsString</code></td>
  <td>false</td>
  <td>
    Some other Parquet-producing systems, in particular Impala and older versions of Spark SQL, do 
    not differentiate between binary data and strings when writing out the Parquet schema.  This 
    flag tells Spark SQL to interpret binary data as a string to provide compatibility with these systems.
  </td>
</tr>
<tr>
  <td><code>spark.sql.parquet.cacheMetadata</code></td>
  <td>true</td>
  <td>
    Turns on caching of Parquet schema metadata.  Can speed up querying of static data.
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
  <td><code>spark.sql.hive.convertMetastoreParquet</code></td>
  <td>true</td>
  <td>
    When set to false, Spark SQL will use the Hive SerDe for parquet tables instead of the built in
    support.
  </td>
</tr>
</table>

## JSON Datasets
<div class="codetabs">

<div data-lang="scala"  markdown="1">
Spark SQL can automatically infer the schema of a JSON dataset and load it as a SchemaRDD.
This conversion can be done using one of two methods in a SQLContext:

* `jsonFile` - loads data from a directory of JSON files where each line of the files is a JSON object.
* `jsonRDD` - loads data from an existing RDD where each element of the RDD is a string containing a JSON object.

{% highlight scala %}
// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// A JSON dataset is pointed to by path.
// The path can be either a single text file or a directory storing text files.
val path = "examples/src/main/resources/people.json"
// Create a SchemaRDD from the file(s) pointed to by path
val people = sqlContext.jsonFile(path)

// The inferred schema can be visualized using the printSchema() method.
people.printSchema()
// root
//  |-- age: integer (nullable = true)
//  |-- name: string (nullable = true)

// Register this SchemaRDD as a table.
people.registerTempTable("people")

// SQL statements can be run by using the sql methods provided by sqlContext.
val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

// Alternatively, a SchemaRDD can be created for a JSON dataset represented by
// an RDD[String] storing one JSON object per string.
val anotherPeopleRDD = sc.parallelize(
  """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
val anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">
Spark SQL can automatically infer the schema of a JSON dataset and load it as a JavaSchemaRDD.
This conversion can be done using one of two methods in a JavaSQLContext :

* `jsonFile` - loads data from a directory of JSON files where each line of the files is a JSON object.
* `jsonRDD` - loads data from an existing RDD where each element of the RDD is a string containing a JSON object.

{% highlight java %}
// sc is an existing JavaSparkContext.
JavaSQLContext sqlContext = new org.apache.spark.sql.api.java.JavaSQLContext(sc);

// A JSON dataset is pointed to by path.
// The path can be either a single text file or a directory storing text files.
String path = "examples/src/main/resources/people.json";
// Create a JavaSchemaRDD from the file(s) pointed to by path
JavaSchemaRDD people = sqlContext.jsonFile(path);

// The inferred schema can be visualized using the printSchema() method.
people.printSchema();
// root
//  |-- age: integer (nullable = true)
//  |-- name: string (nullable = true)

// Register this JavaSchemaRDD as a table.
people.registerTempTable("people");

// SQL statements can be run by using the sql methods provided by sqlContext.
JavaSchemaRDD teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

// Alternatively, a JavaSchemaRDD can be created for a JSON dataset represented by
// an RDD[String] storing one JSON object per string.
List<String> jsonData = Arrays.asList(
  "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
JavaRDD<String> anotherPeopleRDD = sc.parallelize(jsonData);
JavaSchemaRDD anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD);
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
Spark SQL can automatically infer the schema of a JSON dataset and load it as a SchemaRDD.
This conversion can be done using one of two methods in a SQLContext:

* `jsonFile` - loads data from a directory of JSON files where each line of the files is a JSON object.
* `jsonRDD` - loads data from an existing RDD where each element of the RDD is a string containing a JSON object.

{% highlight python %}
# sc is an existing SparkContext.
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files.
path = "examples/src/main/resources/people.json"
# Create a SchemaRDD from the file(s) pointed to by path
people = sqlContext.jsonFile(path)

# The inferred schema can be visualized using the printSchema() method.
people.printSchema()
# root
#  |-- age: integer (nullable = true)
#  |-- name: string (nullable = true)

# Register this SchemaRDD as a table.
people.registerTempTable("people")

# SQL statements can be run by using the sql methods provided by sqlContext.
teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# Alternatively, a SchemaRDD can be created for a JSON dataset represented by
# an RDD[String] storing one JSON object per string.
anotherPeopleRDD = sc.parallelize([
  '{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}'])
anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)
{% endhighlight %}
</div>

</div>

## Hive Tables

Spark SQL also supports reading and writing data stored in [Apache Hive](http://hive.apache.org/).
However, since Hive has a large number of dependencies, it is not included in the default Spark assembly.
Hive support is enabled by adding the `-Phive` and `-Phive-thriftserver` flags to Spark's build.
This command builds a new assembly jar that includes Hive. Note that this Hive assembly jar must also be present
on all of the worker nodes, as they will need access to the Hive serialization and deserialization libraries
(SerDes) in order to access data stored in Hive.

Configuration of Hive is done by placing your `hive-site.xml` file in `conf/`.

<div class="codetabs">

<div data-lang="scala"  markdown="1">

When working with Hive one must construct a `HiveContext`, which inherits from `SQLContext`, and
adds support for finding tables in the MetaStore and writing queries using HiveQL. Users who do
not have an existing Hive deployment can still create a HiveContext.  When not configured by the
hive-site.xml, the context automatically creates `metastore_db` and `warehouse` in the current
directory.

{% highlight scala %}
// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
sqlContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

// Queries are expressed in HiveQL
sqlContext.sql("FROM src SELECT key, value").collect().foreach(println)
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

When working with Hive one must construct a `JavaHiveContext`, which inherits from `JavaSQLContext`, and
adds support for finding tables in the MetaStore and writing queries using HiveQL. In addition to
the `sql` method a `JavaHiveContext` also provides an `hql` methods, which allows queries to be
expressed in HiveQL.

{% highlight java %}
// sc is an existing JavaSparkContext.
JavaHiveContext sqlContext = new org.apache.spark.sql.hive.api.java.HiveContext(sc);

sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
sqlContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src");

// Queries are expressed in HiveQL.
Row[] results = sqlContext.sql("FROM src SELECT key, value").collect();

{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

When working with Hive one must construct a `HiveContext`, which inherits from `SQLContext`, and
adds support for finding tables in the MetaStore and writing queries using HiveQL. In addition to
the `sql` method a `HiveContext` also provides an `hql` methods, which allows queries to be
expressed in HiveQL.

{% highlight python %}
# sc is an existing SparkContext.
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)

sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
sqlContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

# Queries can be expressed in HiveQL.
results = sqlContext.sql("FROM src SELECT key, value").collect()

{% endhighlight %}

</div>
</div>

# Performance Tuning

For some workloads it is possible to improve performance by either caching data in memory, or by
turning on some experimental options.

## Caching Data In Memory

Spark SQL can cache tables using an in-memory columnar format by calling `sqlContext.cacheTable("tableName")`.
Then Spark SQL will scan only required columns and will automatically tune compression to minimize
memory usage and GC pressure. You can call `sqlContext.uncacheTable("tableName")` to remove the table from memory.

Note that if you call `schemaRDD.cache()` rather than `sqlContext.cacheTable(...)`, tables will _not_ be cached using
the in-memory columnar format, and therefore `sqlContext.cacheTable(...)` is strongly recommended for this use case.

Configuration of in-memory caching can be done using the `setConf` method on SQLContext or by running
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
    Controls the size of batches for columnar caching.  Larger batch sizes can improve memory utilization
    and compression, but risk OOMs when caching data.
  </td>
</tr>

</table>

## Other Configuration Options

The following options can also be used to tune the performance of query execution.  It is possible
that these options will be deprecated in future release as more optimizations are performed automatically.

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
  <tr>
    <td><code>spark.sql.autoBroadcastJoinThreshold</code></td>
    <td>10485760 (10 MB)</td>
    <td>
      Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when
      performing a join.  By setting this value to -1 broadcasting can be disabled.  Note that currently
      statistics are only supported for Hive Metastore tables where the command
      `ANALYZE TABLE &lt;tableName&gt; COMPUTE STATISTICS noscan` has been run.
    </td>
  </tr>
  <tr>
    <td><code>spark.sql.codegen</code></td>
    <td>false</td>
    <td>
      When true, code will be dynamically generated at runtime for expression evaluation in a specific
      query.  For some queries with complicated expression this option can lead to significant speed-ups.
      However, for simple queries this can actually slow down query execution.
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

# Other SQL Interfaces

Spark SQL also supports interfaces for running SQL queries directly without the need to write any
code.

## Running the Thrift JDBC/ODBC server

The Thrift JDBC/ODBC server implemented here corresponds to the [`HiveServer2`](https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2)
in Hive 0.12. You can test the JDBC server with the beeline script that comes with either Spark or Hive 0.12.

To start the JDBC/ODBC server, run the following in the Spark directory:

    ./sbin/start-thriftserver.sh

This script accepts all `bin/spark-submit` command line options, plus a `--hiveconf` option to
specify Hive properties.  You may run `./sbin/start-thriftserver.sh --help` for a complete list of
all available options.  By default, the server listens on localhost:10000. You may override this
bahaviour via either environment variables, i.e.:

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

Configuration of Hive is done by placing your `hive-site.xml` file in `conf/`.

You may also use the beeline script that comes with Hive.

## Running the Spark SQL CLI

The Spark SQL CLI is a convenient tool to run the Hive metastore service in local mode and execute
queries input from the command line. Note that the Spark SQL CLI cannot talk to the Thrift JDBC server.

To start the Spark SQL CLI, run the following in the Spark directory:

    ./bin/spark-sql

Configuration of Hive is done by placing your `hive-site.xml` file in `conf/`.
You may run `./bin/spark-sql --help` for a complete list of all available
options.

# Compatibility with Other Systems

## Migration Guide for Shark User

### Scheduling 
To set a [Fair Scheduler](job-scheduling.html#fair-scheduler-pools) pool for a JDBC client session,
users can set the `spark.sql.thriftserver.scheduler.pool` variable:

    SET spark.sql.thriftserver.scheduler.pool=accounting;

### Reducer number

In Shark, default reducer number is 1 and is controlled by the property `mapred.reduce.tasks`. Spark
SQL deprecates this property in favor of `spark.sql.shuffle.partitions`, whose default value
is 200. Users may customize this property via `SET`:

    SET spark.sql.shuffle.partitions=10;
    SELECT page, count(*) c
    FROM logs_last_month_cached
    GROUP BY page ORDER BY c DESC LIMIT 10;

You may also put this property in `hive-site.xml` to override the default value.

For now, the `mapred.reduce.tasks` property is still recognized, and is converted to
`spark.sql.shuffle.partitions` automatically.

### Caching

The `shark.cache` table property no longer exists, and tables whose name end with `_cached` are no
longer automatically cached. Instead, we provide `CACHE TABLE` and `UNCACHE TABLE` statements to
let user control table caching explicitly:

    CACHE TABLE logs_last_month;
    UNCACHE TABLE logs_last_month;

**NOTE:** `CACHE TABLE tbl` is lazy, similar to `.cache` on an RDD. This command only marks `tbl` to ensure that
partitions are cached when calculated but doesn't actually cache it until a query that touches `tbl` is executed.
To force the table to be cached, you may simply count the table immediately after executing `CACHE TABLE`:

    CACHE TABLE logs_last_month;
    SELECT COUNT(1) FROM logs_last_month;

Several caching related features are not supported yet:

* User defined partition level cache eviction policy
* RDD reloading
* In-memory cache write through policy

## Compatibility with Apache Hive

Spark SQL is designed to be compatible with the Hive Metastore, SerDes and UDFs.  Currently Spark
SQL is based on Hive 0.12.0.

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
* Partitioned tables
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

* Tables with partitions using different input formats: In Spark SQL, all table partitions need to
  have the same input format.
* Non-equi outer join: For the uncommon use case of using outer joins with non-equi join conditions
  (e.g. condition "`key < 10`"), Spark SQL will output wrong result for the `NULL` tuple.
* `UNION` type and `DATE` type
* Unique join
* Single query multi insert
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
* Automatically convert a join to map join: For joining a large table with multiple small tables,
  Hive automatically converts the join into a map join. We are adding this auto conversion in the
  next release.
* Automatically determine the number of reducers for joins and groupbys: Currently in Spark SQL, you
  need to control the degree of parallelism post-shuffle using "`SET spark.sql.shuffle.partitions=[num_tasks];`".
* Meta-data only query: For queries that can be answered by using only meta data, Spark SQL still
  launches tasks to compute the result.
* Skew data flag: Spark SQL does not follow the skew data flags in Hive.
* `STREAMTABLE` hint in join: Spark SQL does not follow the `STREAMTABLE` hint.
* Merge multiple small files for query results: if the result output contains multiple small files,
  Hive can optionally merge the small files into fewer large files to avoid overflowing the HDFS
  metadata. Spark SQL does not support that.

# Writing Language-Integrated Relational Queries

**Language-Integrated queries are experimental and currently only supported in Scala.**

Spark SQL also supports a domain specific language for writing queries.  Once again,
using the data from the above examples:

{% highlight scala %}
// sc is an existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// Importing the SQL context gives access to all the public SQL functions and implicit conversions.
import sqlContext._
val people: RDD[Person] = ... // An RDD of case class objects, from the first example.

// The following is the same as 'SELECT name FROM people WHERE age >= 10 AND age <= 19'
val teenagers = people.where('age >= 10).where('age <= 19).select('name)
teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
{% endhighlight %}

The DSL uses Scala symbols to represent columns in the underlying table, which are identifiers
prefixed with a tick (`'`).  Implicit conversions turn these symbols into expressions that are
evaluated by the SQL execution engine.  A full list of the functions supported can be found in the
[ScalaDoc](api/scala/index.html#org.apache.spark.sql.SchemaRDD).

<!-- TODO: Include the table of operations here. -->

# Spark SQL DataType Reference

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

All data types of Spark SQL are located in the package `org.apache.spark.sql`.
You can access them by doing
{% highlight scala %}
import  org.apache.spark.sql._
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
  <td> scala.math.BigDecimal </td>
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
`org.apache.spark.sql.api.java`. To access or create a data type,
please use factory methods provided in
`org.apache.spark.sql.api.java.DataType`.

<table class="table">
<tr>
  <th style="width:20%">Data type</th>
  <th style="width:40%">Value type in Java</th>
  <th>API to access or create a data type</th></tr>
<tr>
  <td> <b>ByteType</b> </td>
  <td> byte or Byte </td>
  <td>
  DataType.ByteType
  </td>
</tr>
<tr>
  <td> <b>ShortType</b> </td>
  <td> short or Short </td>
  <td>
  DataType.ShortType
  </td>
</tr>
<tr>
  <td> <b>IntegerType</b> </td>
  <td> int or Integer </td>
  <td>
  DataType.IntegerType
  </td>
</tr>
<tr>
  <td> <b>LongType</b> </td>
  <td> long or Long </td>
  <td>
  DataType.LongType
  </td>
</tr>
<tr>
  <td> <b>FloatType</b> </td>
  <td> float or Float </td>
  <td>
  DataType.FloatType
  </td>
</tr>
<tr>
  <td> <b>DoubleType</b> </td>
  <td> double or Double </td>
  <td>
  DataType.DoubleType
  </td>
</tr>
<tr>
  <td> <b>DecimalType</b> </td>
  <td> java.math.BigDecimal </td>
  <td>
  DataType.DecimalType
  </td>
</tr>
<tr>
  <td> <b>StringType</b> </td>
  <td> String </td>
  <td>
  DataType.StringType
  </td>
</tr>
<tr>
  <td> <b>BinaryType</b> </td>
  <td> byte[] </td>
  <td>
  DataType.BinaryType
  </td>
</tr>
<tr>
  <td> <b>BooleanType</b> </td>
  <td> boolean or Boolean </td>
  <td>
  DataType.BooleanType
  </td>
</tr>
<tr>
  <td> <b>TimestampType</b> </td>
  <td> java.sql.Timestamp </td>
  <td>
  DataType.TimestampType
  </td>
</tr>
<tr>
  <td> <b>ArrayType</b> </td>
  <td> java.util.List </td>
  <td>
  DataType.createArrayType(<i>elementType</i>)<br />
  <b>Note:</b> The value of <i>containsNull</i> will be <i>true</i><br />
  DataType.createArrayType(<i>elementType</i>, <i>containsNull</i>).
  </td>
</tr>
<tr>
  <td> <b>MapType</b> </td>
  <td> java.util.Map </td>
  <td>
  DataType.createMapType(<i>keyType</i>, <i>valueType</i>)<br />
  <b>Note:</b> The value of <i>valueContainsNull</i> will be <i>true</i>.<br />
  DataType.createMapType(<i>keyType</i>, <i>valueType</i>, <i>valueContainsNull</i>)<br />
  </td>
</tr>
<tr>
  <td> <b>StructType</b> </td>
  <td> org.apache.spark.sql.api.java.Row </td>
  <td>
  DataType.createStructType(<i>fields</i>)<br />
  <b>Note:</b> <i>fields</i> is a List or an array of StructFields.
  Also, two fields with the same name are not allowed.
  </td>
</tr>
<tr>
  <td> <b>StructField</b> </td>
  <td> The value type in Java of the data type of this field
  (For example, int for a StructField with the data type IntegerType) </td>
  <td>
  DataType.createStructField(<i>name</i>, <i>dataType</i>, <i>nullable</i>)
  </td>
</tr>
</table>

</div>

<div data-lang="python"  markdown="1">

All data types of Spark SQL are located in the package of `pyspark.sql`.
You can access them by doing
{% highlight python %}
from pyspark.sql import *
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

</div>

