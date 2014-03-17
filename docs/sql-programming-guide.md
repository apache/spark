---
layout: global
title: Spark SQL Programming Guide
---
**Spark SQL is currently an Alpha component. Therefore, the APIs may be changed in future releases.**

* This will become a table of contents (this text will be scraped).
{:toc}

# Overview
Spark SQL allows relational queries expressed in SQL, HiveQL, or Scala to be executed using
Spark. Tables in Spark SQL can be created from existing RDDs, parquet files, or tables from an
[Apache Hive](http://hive.apache.org/) Metastore.

***************************************************************************************************

# Getting Started

The entry point into all relational functionallity is the
[SQLContext](api/sql/core/index.html#org.apache.spark.sql.SQLContext) class, or one of its
decendents.  To create a basic SQLContext, all you need is a SparkContext.

{% highlight scala %}
val sc: SparkContext // An existing SparkContext.
val sqlContext = new SQLContext(sc)

// Importing the SQL context gives access to all the public SQL functions and implicit conversions.
import sqlContext._
{% endhighlight %}

## Running SQL on RDDs
One type of table that is supported by Spark SQL is an RDD of Scala case classes.  The case class
defines the schema of the table.  The names of the arguments to the case class are read using
reflection and become the names of the columns. Case classes can also be nested or contain complex
types like Seqences or Arrays. Such an RDD can then be registered as a table and used in subsequent SQL
statements.

{% highlight scala %}
// Define the schema using a case class.
case class Person(name: String, age: String)

// Create an RDD of Person objects and register it as a table.
val people: RDD[Person] = sc.textFile("people.txt").map(_.split(",")).map(p => Person(p(0), p(1).toInt))
people.registerAsTable("people")

val teenagers = sql("SELECT name FROM people WHERE age >= 13 && age <= 19")

// The results of SQL queries are themselves RDDs and support all the normal operations
teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
{% endhighlight %}

Note that Spark SQL currently uses a very basic SQL parser.  Users that want a more complete dialect
of SQL should look at the HiveQL support provided by `HiveContext`.

## Using Parquet

Parquet is a columnar format that is supported by many other data processing systems.  Spark SQL
provides support for both reading and writing parquet files that automatically preserves the schema
of the original data.  Using the data from the above example:

{% highlight scala %}
import sqlContext._

// Define the schema using a case class.
case class Person(name: String, age: String)
val people: RDD[Person] // An RDD of case class objects.

// Write out an RDD as a parquet file.
rdd.writeToFile("people.parquet")

// Read in parquet file.  Parquet files are self-describing so the schema is preserved.
val parquetFile = sqlContext.loadFile("people.parquet")

//Parquet files can also be registered as tables.
parquetFile.registerAsTable("parquetFile")
sql("SELECT * FROM parquetFile")
{% endhighlight %}

## Writing Language-Integrated Relational Queries

Spark SQL also supports a domain specific language for writing queries.  Once again,
using the data from the above examples:

{% highlight scala %}
import sqlContext._
val people: RDD[Person] // An RDD of case class objects.

// The following is the same as 'SELECT name FROM people WHERE age >= 10 && age <= 19'
val teenagers = people.where('age >= 10).where('age <= 19).select('name).toRdd
{% endhighlight %}

The DSL uses Scala symbols to represent columns in the underlying table, which are identifiers
prefixed with a tick (`'`).  Implicit conversions turn these symbols into expressions that are
evaluated by the SQL execution engine.  A full list of the functions supported can be found in the
[Scala doc](api/sql/catalyst/org/apache/spark/sql/catalyst/dsl/package$$LogicalPlanFunctions.html).

<!-- TODO: Include the table of operations here. -->

# Hive Support

Spark SQL also supports reading and writing data stored in [Apache Hive](http://hive.apache.org/).
However, since Hive has alarge number of dependencies, it is not included in the default Spark assembly.
In order to use Hive you must first run '`sbt/sbt hive/assembly`'.  This command builds a new assembly
jar that includes Hive and all its dependencies.  When this jar is present, Spark will use the Hive
assembly instead of the normal Spark assembly.  Note that this Hive assembly jar must also be present
on all of the worker nodes, as they will need access to the Hive serialization and deserialization libraries
(SerDes) in order to acccess data stored in Hive.

Configuration of Hive is done by placing your `hive-site.xml` file in `conf/`.

When working with Hive one must construct a `HiveContext`, which inherits from `SQLContext`, and
adds support for finding tables in in the MetaStore and writing queries using HiveQL. Users who do
not have an existing Hive deployment can also experiment with the `LocalHiveContext`,
which is similar to `HiveContext`, but creates a local copy of the `metastore` and `warehouse`
automatically.

{% highlight scala %}
val sc: SparkContext // An existing SparkContext.
val hiveContext = new HiveContext(sc)

// Importing the SQL context gives access to all the public SQL functions and implicit conversions.
import hiveContext._

sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src")

// Queries are expressed in HiveQL
sql("SELECT key, value FROM src").collect().foreach(println)
{% endhighlight %}
