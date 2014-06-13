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
[SchemaRDD](api/scala/index.html#org.apache.spark.sql.SchemaRDD).  SchemaRDDs are composed
[Row](api/scala/index.html#org.apache.spark.sql.catalyst.expressions.Row) objects along with
a schema that describes the data types of each column in the row.  A SchemaRDD is similar to a table
in a traditional relational database.  A SchemaRDD can be created from an existing RDD, [Parquet](http://parquet.io)
file, or by running HiveQL against data stored in [Apache Hive](http://hive.apache.org/).

All of the examples on this page use sample data included in the Spark distribution and can be run in the `spark-shell`.

</div>

<div data-lang="java"  markdown="1">
Spark SQL allows relational queries expressed in SQL, HiveQL, or Scala to be executed using
Spark.  At the core of this component is a new type of RDD,
[JavaSchemaRDD](api/scala/index.html#org.apache.spark.sql.api.java.JavaSchemaRDD).  JavaSchemaRDDs are composed
[Row](api/scala/index.html#org.apache.spark.sql.api.java.Row) objects along with
a schema that describes the data types of each column in the row.  A JavaSchemaRDD is similar to a table
in a traditional relational database.  A JavaSchemaRDD can be created from an existing RDD, [Parquet](http://parquet.io)
file, or by running HiveQL against data stored in [Apache Hive](http://hive.apache.org/).
</div>

<div data-lang="python"  markdown="1">

Spark SQL allows relational queries expressed in SQL or HiveQL to be executed using
Spark.  At the core of this component is a new type of RDD,
[SchemaRDD](api/python/pyspark.sql.SchemaRDD-class.html).  SchemaRDDs are composed
[Row](api/python/pyspark.sql.Row-class.html) objects along with
a schema that describes the data types of each column in the row.  A SchemaRDD is similar to a table
in a traditional relational database.  A SchemaRDD can be created from an existing RDD, [Parquet](http://parquet.io)
file, or by running HiveQL against data stored in [Apache Hive](http://hive.apache.org/).

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

// Importing the SQL context gives access to all the public SQL functions and implicit conversions.
import sqlContext._
{% endhighlight %}

</div>

<div data-lang="java" markdown="1">

The entry point into all relational functionality in Spark is the
[JavaSQLContext](api/scala/index.html#org.apache.spark.sql.api.java.JavaSQLContext) class, or one
of its descendants.  To create a basic JavaSQLContext, all you need is a JavaSparkContext.

{% highlight java %}
JavaSparkContext ctx = ...; // An existing JavaSparkContext.
JavaSQLContext sqlCtx = new org.apache.spark.sql.api.java.JavaSQLContext(ctx);
{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

The entry point into all relational functionality in Spark is the
[SQLContext](api/python/pyspark.sql.SQLContext-class.html) class, or one
of its decedents.  To create a basic SQLContext, all you need is a SparkContext.

{% highlight python %}
from pyspark.sql import SQLContext
sqlCtx = SQLContext(sc)
{% endhighlight %}

</div>

</div>

## Running SQL on RDDs

<div class="codetabs">

<div data-lang="scala"  markdown="1">

One type of table that is supported by Spark SQL is an RDD of Scala case classes.  The case class
defines the schema of the table.  The names of the arguments to the case class are read using
reflection and become the names of the columns. Case classes can also be nested or contain complex
types such as Sequences or Arrays. This RDD can be implicitly converted to a SchemaRDD and then be
registered as a table.  Tables can be used in subsequent SQL statements.

{% highlight scala %}
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext._

// Define the schema using a case class.
// Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit, 
// you can use custom classes that implement the Product interface.
case class Person(name: String, age: Int)

// Create an RDD of Person objects and register it as a table.
val people = sc.textFile("examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
people.registerAsTable("people")

// SQL statements can be run by using the sql methods provided by sqlContext.
val teenagers = sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

// The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
// The columns of a row in the result can be accessed by ordinal.
teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

One type of table that is supported by Spark SQL is an RDD of [JavaBeans](http://stackoverflow.com/questions/3295496/what-is-a-javabean-exactly).  The BeanInfo
defines the schema of the table. Currently, Spark SQL does not support JavaBeans that contain
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

JavaSparkContext ctx = ...; // An existing JavaSparkContext.
JavaSQLContext sqlCtx = new org.apache.spark.sql.api.java.JavaSQLContext(ctx)

// Load a text file and convert each line to a JavaBean.
JavaRDD<Person> people = ctx.textFile("examples/src/main/resources/people.txt").map(
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
JavaSchemaRDD schemaPeople = sqlCtx.applySchema(people, Person.class);
schemaPeople.registerAsTable("people");

// SQL can be run over RDDs that have been registered as tables.
JavaSchemaRDD teenagers = sqlCtx.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

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

One type of table that is supported by Spark SQL is an RDD of dictionaries.  The keys of the
dictionary define the columns names of the table, and the types are inferred by looking at the first
row. Any RDD of dictionaries can converted to a SchemaRDD and then registered as a table.  Tables
can be used in subsequent SQL statements.

{% highlight python %}
# Load a text file and convert each line to a dictionary.
lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: {"name": p[0], "age": int(p[1])})

# Infer the schema, and register the SchemaRDD as a table.
# In future versions of PySpark we would like to add support for registering RDDs with other
# datatypes as tables
peopleTable = sqlCtx.inferSchema(people)
peopleTable.registerAsTable("people")

# SQL can be run over SchemaRDDs that have been registered as a table.
teenagers = sqlCtx.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
teenNames = teenagers.map(lambda p: "Name: " + p.name)
{% endhighlight %}

</div>

</div>

**Note that Spark SQL currently uses a very basic SQL parser.**
Users that want a more complete dialect of SQL should look at the HiveQL support provided by
`HiveContext`.

## Running SQL on JSON Datasets
**Running SQL on JSON datasets is currently only supported in Scala.**

Spark SQL supports querying JSON datasets. To query a JSON dataset, a SchemaRDD needs to be created for this JSON dataset. There are three ways:

1. Creating the SchemaRDD from text files that store one JSON object per line by using `jsonFile`.
2. Creating the SchemaRDD from a RDD of strings (`RDD[String]`) that stores one JSON object per string by using `jsonRDD`.
3. Creating the SchemaRDD by unioning two SchemaRDDs for two JSON datasets through `schemaRDD1.unionAll(schemaRDD2)`.

The schema (field names and types) of a JSON dataset is automatically inferred when the SchemaRDD is created.

{% highlight scala %}
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext._

// A JSON dataset is pointed by path.
// The path can be either a single text file or a directory storing text files.
val path = "examples/src/main/resources/people.json"
// Create a SchemaRDD from the file(s) pointed by path
val people = jsonFile(path)

// Because the schema of a JSON dataset is automatically inferred, to write queries,
// it is better to take a look at what is the schema.
people.printSchema()
// The schema of people is ...
// root
//  |-- age: IntegerType
//  |-- name: StringType

// Register this SchemaRDD as a table.
people.registerAsTable("people")

// SQL statements can be run by using the sql methods provided by sqlContext.
val teenagers = sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

// The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
// The columns of a row in the result can be accessed by ordinal.
teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

// Let's create another SchemaRDD from a RDD[String] storing one JSON object per string.
val anotherPeopleRDD = sc.parallelize(
  """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
val anotherPeople = jsonRDD(anotherPeopleRDD)

// Take a look at the schema of this new SchemaRDD.
anotherPeople.printSchema()
// The schema of anotherPeople is ...
// root
//  |-- address: StructType
//  |    |-- city: StringType
//  |    |-- state: StringType
//  |-- name: StringType
 
// Two SchemaRDDs created from JSON datasets can be unioned together.
val peopleUnioned = people.unionAll(anotherPeople)

// Take a look at the schema of this unioned SchemaRDD.
peopleUnioned.printSchema()
// The schema of this unioned SchemaRDD is ...
// root
//  |-- address: StructType
//  |    |-- city: StringType
//  |    |-- state: StringType
//  |-- age: IntegerType
//  |-- name: StringType

// Register this unioned SchemaRDD as a table.
peopleUnioned.registerAsTable("peopleUnioned")
val peopleFromColumbus =
  sql("SELECT name, address.city FROM peopleUnioned WHERE address.city = 'Columbus'")

// See the result.
peopleFromColumbus.map(t => "Name: " + t(0) + ", City: " + t(1)).collect().foreach(println)

{% endhighlight %}


## Using Parquet

[Parquet](http://parquet.io) is a columnar format that is supported by many other data processing systems.
Spark SQL provides support for both reading and writing Parquet files that automatically preserves the schema
of the original data.  Using the data from the above example:

<div class="codetabs">

<div data-lang="scala"  markdown="1">

{% highlight scala %}
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext._

val people: RDD[Person] = ... // An RDD of case class objects, from the previous example.

// The RDD is implicitly converted to a SchemaRDD, allowing it to be stored using Parquet.
people.saveAsParquetFile("people.parquet")

// Read in the parquet file created above.  Parquet files are self-describing so the schema is preserved.
// The result of loading a Parquet file is also a JavaSchemaRDD.
val parquetFile = sqlContext.parquetFile("people.parquet")

//Parquet files can also be registered as tables and then used in SQL statements.
parquetFile.registerAsTable("parquetFile")
val teenagers = sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
teenagers.collect().foreach(println)
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}

JavaSchemaRDD schemaPeople = ... // The JavaSchemaRDD from the previous example.

// JavaSchemaRDDs can be saved as Parquet files, maintaining the schema information.
schemaPeople.saveAsParquetFile("people.parquet");

// Read in the Parquet file created above.  Parquet files are self-describing so the schema is preserved.
// The result of loading a parquet file is also a JavaSchemaRDD.
JavaSchemaRDD parquetFile = sqlCtx.parquetFile("people.parquet");

//Parquet files can also be registered as tables and then used in SQL statements.
parquetFile.registerAsTable("parquetFile");
JavaSchemaRDD teenagers = sqlCtx.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19");


{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

{% highlight python %}

peopleTable # The SchemaRDD from the previous example.

# SchemaRDDs can be saved as Parquet files, maintaining the schema information.
peopleTable.saveAsParquetFile("people.parquet")

# Read in the Parquet file created above.  Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a SchemaRDD.
parquetFile = sqlCtx.parquetFile("people.parquet")

# Parquet files can also be registered as tables and then used in SQL statements.
parquetFile.registerAsTable("parquetFile");
teenagers = sqlCtx.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")

{% endhighlight %}

</div>

</div>

## Writing Language-Integrated Relational Queries

**Language-Integrated queries are currently only supported in Scala.**

Spark SQL also supports a domain specific language for writing queries.  Once again,
using the data from the above examples:

{% highlight scala %}
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext._
val people: RDD[Person] = ... // An RDD of case class objects, from the first example.

// The following is the same as 'SELECT name FROM people WHERE age >= 10 AND age <= 19'
val teenagers = people.where('age >= 10).where('age <= 19).select('name)
{% endhighlight %}

The DSL uses Scala symbols to represent columns in the underlying table, which are identifiers
prefixed with a tick (`'`).  Implicit conversions turn these symbols into expressions that are
evaluated by the SQL execution engine.  A full list of the functions supported can be found in the
[ScalaDoc](api/scala/index.html#org.apache.spark.sql.SchemaRDD).

<!-- TODO: Include the table of operations here. -->

# Hive Support

Spark SQL also supports reading and writing data stored in [Apache Hive](http://hive.apache.org/).
However, since Hive has a large number of dependencies, it is not included in the default Spark assembly.
In order to use Hive you must first run '`SPARK_HIVE=true sbt/sbt assembly/assembly`' (or use `-Phive` for maven).
This command builds a new assembly jar that includes Hive. Note that this Hive assembly jar must also be present
on all of the worker nodes, as they will need access to the Hive serialization and deserialization libraries
(SerDes) in order to acccess data stored in Hive.

Configuration of Hive is done by placing your `hive-site.xml` file in `conf/`.

<div class="codetabs">

<div data-lang="scala"  markdown="1">

When working with Hive one must construct a `HiveContext`, which inherits from `SQLContext`, and
adds support for finding tables in in the MetaStore and writing queries using HiveQL. Users who do
not have an existing Hive deployment can also experiment with the `LocalHiveContext`,
which is similar to `HiveContext`, but creates a local copy of the `metastore` and `warehouse`
automatically.

{% highlight scala %}
val sc: SparkContext // An existing SparkContext.
val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

// Importing the SQL context gives access to all the public SQL functions and implicit conversions.
import hiveContext._

hql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
hql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

// Queries are expressed in HiveQL
hql("FROM src SELECT key, value").collect().foreach(println)
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

When working with Hive one must construct a `JavaHiveContext`, which inherits from `JavaSQLContext`, and
adds support for finding tables in in the MetaStore and writing queries using HiveQL. In addition to
the `sql` method a `JavaHiveContext` also provides an `hql` methods, which allows queries to be
expressed in HiveQL.

{% highlight java %}
JavaSparkContext ctx = ...; // An existing JavaSparkContext.
JavaHiveContext hiveCtx = new org.apache.spark.sql.hive.api.java.HiveContext(ctx);

hiveCtx.hql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
hiveCtx.hql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src");

// Queries are expressed in HiveQL.
Row[] results = hiveCtx.hql("FROM src SELECT key, value").collect();

{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

When working with Hive one must construct a `HiveContext`, which inherits from `SQLContext`, and
adds support for finding tables in in the MetaStore and writing queries using HiveQL. In addition to
the `sql` method a `HiveContext` also provides an `hql` methods, which allows queries to be
expressed in HiveQL.

{% highlight python %}

from pyspark.sql import HiveContext
hiveCtx = HiveContext(sc)

hiveCtx.hql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
hiveCtx.hql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

# Queries can be expressed in HiveQL.
results = hiveCtx.hql("FROM src SELECT key, value").collect()

{% endhighlight %}

</div>
</div>
