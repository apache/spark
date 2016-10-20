/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.beans.BeanInfo
import java.util.Properties

import scala.collection.immutable
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation.{DeveloperApi, Experimental, InterfaceStability}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.command.ShowTablesCommand
import org.apache.spark.sql.internal.{SessionState, SharedState, SQLConf}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQueryManager}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ExecutionListenerManager

/**
 * The entry point for working with structured data (rows and columns) in Spark 1.x.
 *
 * As of Spark 2.0, this is replaced by [[SparkSession]]. However, we are keeping the class
 * here for backward compatibility.
 *
 * @groupname basic Basic Operations
 * @groupname ddl_ops Persistent Catalog DDL
 * @groupname cachemgmt Cached Table Management
 * @groupname genericdata Generic Data Sources
 * @groupname specificdata Specific Data Sources
 * @groupname config Configuration
 * @groupname dataframes Custom DataFrame Creation
 * @groupname dataset Custom Dataset Creation
 * @groupname Ungrouped Support functions for language integrated queries
 * @since 1.0.0
 */
@InterfaceStability.Stable
class SQLContext private[sql](val sparkSession: SparkSession)
  extends Logging with Serializable {

  self =>

  sparkSession.sparkContext.assertNotStopped()

  // Note: Since Spark 2.0 this class has become a wrapper of SparkSession, where the
  // real functionality resides. This class remains mainly for backward compatibility.

  @deprecated("Use SparkSession.builder instead", "2.0.0")
  def this(sc: SparkContext) = {
    this(SparkSession.builder().sparkContext(sc).getOrCreate())
  }

  @deprecated("Use SparkSession.builder instead", "2.0.0")
  def this(sparkContext: JavaSparkContext) = this(sparkContext.sc)

  // TODO: move this logic into SparkSession

  private[sql] def sessionState: SessionState = sparkSession.sessionState
  private[sql] def sharedState: SharedState = sparkSession.sharedState
  private[sql] def conf: SQLConf = sessionState.conf

  def sparkContext: SparkContext = sparkSession.sparkContext

  /**
   * Returns a [[SQLContext]] as new session, with separated SQL configurations, temporary
   * tables, registered functions, but sharing the same [[SparkContext]], cached data and
   * other things.
   *
   * @since 1.6.0
   */
  def newSession(): SQLContext = sparkSession.newSession().sqlContext

  /**
   * An interface to register custom [[org.apache.spark.sql.util.QueryExecutionListener]]s
   * that listen for execution metrics.
   */
  @Experimental
  @InterfaceStability.Evolving
  def listenerManager: ExecutionListenerManager = sparkSession.listenerManager

  /**
   * Set Spark SQL configuration properties.
   *
   * @group config
   * @since 1.0.0
   */
  def setConf(props: Properties): Unit = {
    sessionState.conf.setConf(props)
  }

  /**
   * Set the given Spark SQL configuration property.
   */
  private[sql] def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    sessionState.conf.setConf(entry, value)
  }

  /**
   * Set the given Spark SQL configuration property.
   *
   * @group config
   * @since 1.0.0
   */
  def setConf(key: String, value: String): Unit = {
    sparkSession.conf.set(key, value)
  }

  /**
   * Return the value of Spark SQL configuration property for the given key.
   *
   * @group config
   * @since 1.0.0
   */
  def getConf(key: String): String = {
    sparkSession.conf.get(key)
  }

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue`.
   *
   * @group config
   * @since 1.0.0
   */
  def getConf(key: String, defaultValue: String): String = {
    sparkSession.conf.get(key, defaultValue)
  }

  /**
   * Return all the configuration properties that have been set (i.e. not the default).
   * This creates a new copy of the config properties in the form of a Map.
   *
   * @group config
   * @since 1.0.0
   */
  def getAllConfs: immutable.Map[String, String] = {
    sparkSession.conf.getAll
  }

  /**
   * :: Experimental ::
   * A collection of methods that are considered experimental, but can be used to hook into
   * the query planner for advanced functionality.
   *
   * @group basic
   * @since 1.3.0
   */
  @Experimental
  @transient
  @InterfaceStability.Unstable
  def experimental: ExperimentalMethods = sparkSession.experimental

  /**
   * Returns a [[DataFrame]] with no rows or columns.
   *
   * @group basic
   * @since 1.3.0
   */
  def emptyDataFrame: DataFrame = sparkSession.emptyDataFrame

  /**
   * A collection of methods for registering user-defined functions (UDF).
   * Note that the user-defined functions must be deterministic. Due to optimization,
   * duplicate invocations may be eliminated or the function may even be invoked more times than
   * it is present in the query.
   *
   * The following example registers a Scala closure as UDF:
   * {{{
   *   sqlContext.udf.register("myUDF", (arg1: Int, arg2: String) => arg2 + arg1)
   * }}}
   *
   * The following example registers a UDF in Java:
   * {{{
   *   sqlContext.udf().register("myUDF",
   *       new UDF2<Integer, String, String>() {
   *           @Override
   *           public String call(Integer arg1, String arg2) {
   *               return arg2 + arg1;
   *           }
   *      }, DataTypes.StringType);
   * }}}
   *
   * Or, to use Java 8 lambda syntax:
   * {{{
   *   sqlContext.udf().register("myUDF",
   *       (Integer arg1, String arg2) -> arg2 + arg1,
   *       DataTypes.StringType);
   * }}}
   *
   * @group basic
   * @since 1.3.0
   */
  def udf: UDFRegistration = sparkSession.udf

  /**
   * Returns true if the table is currently cached in-memory.
   * @group cachemgmt
   * @since 1.3.0
   */
  def isCached(tableName: String): Boolean = {
    sparkSession.catalog.isCached(tableName)
  }

  /**
   * Caches the specified table in-memory.
   * @group cachemgmt
   * @since 1.3.0
   */
  def cacheTable(tableName: String): Unit = {
    sparkSession.catalog.cacheTable(tableName)
  }

  /**
   * Removes the specified table from the in-memory cache.
   * @group cachemgmt
   * @since 1.3.0
   */
  def uncacheTable(tableName: String): Unit = {
    sparkSession.catalog.uncacheTable(tableName)
  }

  /**
   * Removes all cached tables from the in-memory cache.
   * @since 1.3.0
   */
  def clearCache(): Unit = {
    sparkSession.catalog.clearCache()
  }

  // scalastyle:off
  // Disable style checker so "implicits" object can start with lowercase i
  /**
   * :: Experimental ::
   * (Scala-specific) Implicit methods available in Scala for converting
   * common Scala objects into [[DataFrame]]s.
   *
   * {{{
   *   val sqlContext = new SQLContext(sc)
   *   import sqlContext.implicits._
   * }}}
   *
   * @group basic
   * @since 1.3.0
   */
  @Experimental
  @InterfaceStability.Evolving
  object implicits extends SQLImplicits with Serializable {
    protected override def _sqlContext: SQLContext = self
  }
  // scalastyle:on

  /**
   * :: Experimental ::
   * Creates a DataFrame from an RDD of Product (e.g. case classes, tuples).
   *
   * @group dataframes
   * @since 1.3.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def createDataFrame[A <: Product : TypeTag](rdd: RDD[A]): DataFrame = {
    sparkSession.createDataFrame(rdd)
  }

  /**
   * :: Experimental ::
   * Creates a DataFrame from a local Seq of Product.
   *
   * @group dataframes
   * @since 1.3.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def createDataFrame[A <: Product : TypeTag](data: Seq[A]): DataFrame = {
    sparkSession.createDataFrame(data)
  }

  /**
   * Convert a [[BaseRelation]] created for external data sources into a [[DataFrame]].
   *
   * @group dataframes
   * @since 1.3.0
   */
  def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame = {
    sparkSession.baseRelationToDataFrame(baseRelation)
  }

  /**
   * :: DeveloperApi ::
   * Creates a [[DataFrame]] from an [[RDD]] containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided RDD matches
   * the provided schema. Otherwise, there will be runtime exception.
   * Example:
   * {{{
   *  import org.apache.spark.sql._
   *  import org.apache.spark.sql.types._
   *  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   *
   *  val schema =
   *    StructType(
   *      StructField("name", StringType, false) ::
   *      StructField("age", IntegerType, true) :: Nil)
   *
   *  val people =
   *    sc.textFile("examples/src/main/resources/people.txt").map(
   *      _.split(",")).map(p => Row(p(0), p(1).trim.toInt))
   *  val dataFrame = sqlContext.createDataFrame(people, schema)
   *  dataFrame.printSchema
   *  // root
   *  // |-- name: string (nullable = false)
   *  // |-- age: integer (nullable = true)
   *
   *  dataFrame.createOrReplaceTempView("people")
   *  sqlContext.sql("select name from people").collect.foreach(println)
   * }}}
   *
   * @group dataframes
   * @since 1.3.0
   */
  @DeveloperApi
  @InterfaceStability.Evolving
  def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame = {
    sparkSession.createDataFrame(rowRDD, schema)
  }

  /**
   * Creates a DataFrame from an RDD[Row]. User can specify whether the input rows should be
   * converted to Catalyst rows.
   */
  private[sql]
  def createDataFrame(rowRDD: RDD[Row], schema: StructType, needsConversion: Boolean) = {
    sparkSession.createDataFrame(rowRDD, schema, needsConversion)
  }

  /**
   * :: Experimental ::
   * Creates a [[Dataset]] from a local Seq of data of a given type. This method requires an
   * encoder (to convert a JVM object of type `T` to and from the internal Spark SQL representation)
   * that is generally created automatically through implicits from a `SparkSession`, or can be
   * created explicitly by calling static methods on [[Encoders]].
   *
   * == Example ==
   *
   * {{{
   *
   *   import spark.implicits._
   *   case class Person(name: String, age: Long)
   *   val data = Seq(Person("Michael", 29), Person("Andy", 30), Person("Justin", 19))
   *   val ds = spark.createDataset(data)
   *
   *   ds.show()
   *   // +-------+---+
   *   // |   name|age|
   *   // +-------+---+
   *   // |Michael| 29|
   *   // |   Andy| 30|
   *   // | Justin| 19|
   *   // +-------+---+
   * }}}
   *
   * @since 2.0.0
   * @group dataset
   */
  @Experimental
  @InterfaceStability.Evolving
  def createDataset[T : Encoder](data: Seq[T]): Dataset[T] = {
    sparkSession.createDataset(data)
  }

  /**
   * :: Experimental ::
   * Creates a [[Dataset]] from an RDD of a given type. This method requires an
   * encoder (to convert a JVM object of type `T` to and from the internal Spark SQL representation)
   * that is generally created automatically through implicits from a `SparkSession`, or can be
   * created explicitly by calling static methods on [[Encoders]].
   *
   * @since 2.0.0
   * @group dataset
   */
  @Experimental
  def createDataset[T : Encoder](data: RDD[T]): Dataset[T] = {
    sparkSession.createDataset(data)
  }

  /**
   * :: Experimental ::
   * Creates a [[Dataset]] from a [[java.util.List]] of a given type. This method requires an
   * encoder (to convert a JVM object of type `T` to and from the internal Spark SQL representation)
   * that is generally created automatically through implicits from a `SparkSession`, or can be
   * created explicitly by calling static methods on [[Encoders]].
   *
   * == Java Example ==
   *
   * {{{
   *     List<String> data = Arrays.asList("hello", "world");
   *     Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
   * }}}
   *
   * @since 2.0.0
   * @group dataset
   */
  @Experimental
  @InterfaceStability.Evolving
  def createDataset[T : Encoder](data: java.util.List[T]): Dataset[T] = {
    sparkSession.createDataset(data)
  }

  /**
   * Creates a DataFrame from an RDD[Row]. User can specify whether the input rows should be
   * converted to Catalyst rows.
   */
  private[sql]
  def internalCreateDataFrame(catalystRows: RDD[InternalRow], schema: StructType) = {
    sparkSession.internalCreateDataFrame(catalystRows, schema)
  }

  /**
   * :: DeveloperApi ::
   * Creates a [[DataFrame]] from a [[JavaRDD]] containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided RDD matches
   * the provided schema. Otherwise, there will be runtime exception.
   *
   * @group dataframes
   * @since 1.3.0
   */
  @DeveloperApi
  @InterfaceStability.Evolving
  def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame = {
    sparkSession.createDataFrame(rowRDD, schema)
  }

  /**
   * :: DeveloperApi ::
   * Creates a [[DataFrame]] from a [[java.util.List]] containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided List matches
   * the provided schema. Otherwise, there will be runtime exception.
   *
   * @group dataframes
   * @since 1.6.0
   */
  @DeveloperApi
  @InterfaceStability.Evolving
  def createDataFrame(rows: java.util.List[Row], schema: StructType): DataFrame = {
    sparkSession.createDataFrame(rows, schema)
  }

  /**
   * Applies a schema to an RDD of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   *          SELECT * queries will return the columns in an undefined order.
   * @group dataframes
   * @since 1.3.0
   */
  def createDataFrame(rdd: RDD[_], beanClass: Class[_]): DataFrame = {
    sparkSession.createDataFrame(rdd, beanClass)
  }

  /**
   * Applies a schema to an RDD of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   *          SELECT * queries will return the columns in an undefined order.
   * @group dataframes
   * @since 1.3.0
   */
  def createDataFrame(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame = {
    sparkSession.createDataFrame(rdd, beanClass)
  }

  /**
   * Applies a schema to a List of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   *          SELECT * queries will return the columns in an undefined order.
   * @group dataframes
   * @since 1.6.0
   */
  def createDataFrame(data: java.util.List[_], beanClass: Class[_]): DataFrame = {
    sparkSession.createDataFrame(data, beanClass)
  }

  /**
   * Returns a [[DataFrameReader]] that can be used to read non-streaming data in as a
   * [[DataFrame]].
   * {{{
   *   sqlContext.read.parquet("/path/to/file.parquet")
   *   sqlContext.read.schema(schema).json("/path/to/file.json")
   * }}}
   *
   * @group genericdata
   * @since 1.4.0
   */
  def read: DataFrameReader = sparkSession.read


  /**
   * :: Experimental ::
   * Returns a [[DataStreamReader]] that can be used to read streaming data in as a [[DataFrame]].
   * {{{
   *   sparkSession.readStream.parquet("/path/to/directory/of/parquet/files")
   *   sparkSession.readStream.schema(schema).json("/path/to/directory/of/json/files")
   * }}}
   *
   * @since 2.0.0
   */
  @Experimental
  @InterfaceStability.Evolving
  def readStream: DataStreamReader = sparkSession.readStream


  /**
   * Creates an external table from the given path and returns the corresponding DataFrame.
   * It will use the default data source configured by spark.sql.sources.default.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def createExternalTable(tableName: String, path: String): DataFrame = {
    sparkSession.catalog.createExternalTable(tableName, path)
  }

  /**
   * Creates an external table from the given path based on a data source
   * and returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def createExternalTable(
      tableName: String,
      path: String,
      source: String): DataFrame = {
    sparkSession.catalog.createExternalTable(tableName, path, source)
  }

  /**
   * Creates an external table from the given path based on a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def createExternalTable(
      tableName: String,
      source: String,
      options: java.util.Map[String, String]): DataFrame = {
    sparkSession.catalog.createExternalTable(tableName, source, options)
  }

  /**
   * (Scala-specific)
   * Creates an external table from the given path based on a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def createExternalTable(
      tableName: String,
      source: String,
      options: Map[String, String]): DataFrame = {
    sparkSession.catalog.createExternalTable(tableName, source, options)
  }

  /**
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: java.util.Map[String, String]): DataFrame = {
    sparkSession.catalog.createExternalTable(tableName, source, schema, options)
  }

  /**
   * (Scala-specific)
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame = {
    sparkSession.catalog.createExternalTable(tableName, source, schema, options)
  }

  /**
   * Registers the given [[DataFrame]] as a temporary table in the catalog. Temporary tables exist
   * only during the lifetime of this instance of SQLContext.
   */
  private[sql] def registerDataFrameAsTable(df: DataFrame, tableName: String): Unit = {
    df.createOrReplaceTempView(tableName)
  }

  /**
   * Drops the temporary table with the given table name in the catalog. If the table has been
   * cached/persisted before, it's also unpersisted.
   *
   * @param tableName the name of the table to be unregistered.
   * @group basic
   * @since 1.3.0
   */
  def dropTempTable(tableName: String): Unit = {
    sparkSession.catalog.dropTempView(tableName)
  }

  /**
   * :: Experimental ::
   * Creates a [[DataFrame]] with a single [[LongType]] column named `id`, containing elements
   * in a range from 0 to `end` (exclusive) with step value 1.
   *
   * @since 1.4.1
   * @group dataframe
   */
  @Experimental
  @InterfaceStability.Evolving
  def range(end: Long): DataFrame = sparkSession.range(end).toDF()

  /**
   * :: Experimental ::
   * Creates a [[DataFrame]] with a single [[LongType]] column named `id`, containing elements
   * in a range from `start` to `end` (exclusive) with step value 1.
   *
   * @since 1.4.0
   * @group dataframe
   */
  @Experimental
  @InterfaceStability.Evolving
  def range(start: Long, end: Long): DataFrame = sparkSession.range(start, end).toDF()

  /**
   * :: Experimental ::
   * Creates a [[DataFrame]] with a single [[LongType]] column named `id`, containing elements
   * in a range from `start` to `end` (exclusive) with a step value.
   *
   * @since 2.0.0
   * @group dataframe
   */
  @Experimental
  @InterfaceStability.Evolving
  def range(start: Long, end: Long, step: Long): DataFrame = {
    sparkSession.range(start, end, step).toDF()
  }

  /**
   * :: Experimental ::
   * Creates a [[DataFrame]] with a single [[LongType]] column named `id`, containing elements
   * in an range from `start` to `end` (exclusive) with an step value, with partition number
   * specified.
   *
   * @since 1.4.0
   * @group dataframe
   */
  @Experimental
  @InterfaceStability.Evolving
  def range(start: Long, end: Long, step: Long, numPartitions: Int): DataFrame = {
    sparkSession.range(start, end, step, numPartitions).toDF()
  }

  /**
   * Executes a SQL query using Spark, returning the result as a [[DataFrame]]. The dialect that is
   * used for SQL parsing can be configured with 'spark.sql.dialect'.
   *
   * @group basic
   * @since 1.3.0
   */
  def sql(sqlText: String): DataFrame = sparkSession.sql(sqlText)

  /**
   * Returns the specified table as a [[DataFrame]].
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def table(tableName: String): DataFrame = {
    sparkSession.table(tableName)
  }

  /**
   * Returns the specified database table as a [[DataFrame]]
   * @group ddl_ops
   * @since 1.5.0
   */
  def table(databaseName: String, tableName: String): DataFrame =
    DataFrame(this, catalog.lookupRelation(Seq(databaseName, tableName)))


  /**
   * Returns a [[DataFrame]] containing names of existing tables in the current database.
   * The returned DataFrame has two columns, tableName and isTemporary (a Boolean
   * indicating if a table is a temporary one or not).
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def tables(): DataFrame = {
    Dataset.ofRows(sparkSession, ShowTablesCommand(None, None))
  }

  /**
   * Returns a [[DataFrame]] containing names of existing tables in the given database.
   * The returned DataFrame has two columns, tableName and isTemporary (a Boolean
   * indicating if a table is a temporary one or not).
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def tables(databaseName: String): DataFrame = {
    Dataset.ofRows(sparkSession, ShowTablesCommand(Some(databaseName), None))
  }

  /**
   * Returns a [[StreamingQueryManager]] that allows managing all the
   * [[org.apache.spark.sql.streaming.StreamingQuery StreamingQueries]] active on `this` context.
   *
   * @since 2.0.0
   */
  def streams: StreamingQueryManager = sparkSession.streams

  /**
   * Returns the names of tables in the current database as an array.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def tableNames(): Array[String] = {
    sparkSession.catalog.listTables().collect().map(_.name)
  }

  /**
   * Returns the names of tables in the given database as an array.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def tableNames(databaseName: String): Array[String] = {
    sparkSession.catalog.listTables(databaseName).collect().map(_.name)
  }

  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////
  // Deprecated methods
  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////

  /**
   * @deprecated As of 1.3.0, replaced by `createDataFrame()`.
   */
  @deprecated("Use createDataFrame instead.", "1.3.0")
  def applySchema(rowRDD: RDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD, schema)
  }

  /**
   * @deprecated As of 1.3.0, replaced by `createDataFrame()`.
   */
  @deprecated("Use createDataFrame instead.", "1.3.0")
  def applySchema(rowRDD: JavaRDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD, schema)
  }

  /**
   * @deprecated As of 1.3.0, replaced by `createDataFrame()`.
   */
  @deprecated("Use createDataFrame instead.", "1.3.0")
  def applySchema(rdd: RDD[_], beanClass: Class[_]): DataFrame = {
    createDataFrame(rdd, beanClass)
  }

  /**
   * @deprecated As of 1.3.0, replaced by `createDataFrame()`.
   */
  @deprecated("Use createDataFrame instead.", "1.3.0")
  def applySchema(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame = {
    createDataFrame(rdd, beanClass)
  }

  /**
   * Loads a Parquet file, returning the result as a [[DataFrame]]. This function returns an empty
   * [[DataFrame]] if no paths are passed in.
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().parquet()`.
   */
  @deprecated("Use read.parquet() instead.", "1.4.0")
  @scala.annotation.varargs
  def parquetFile(paths: String*): DataFrame = {
    if (paths.isEmpty) {
      emptyDataFrame
    } else {
      read.parquet(paths : _*)
    }
  }

  /**
   * Loads a JSON file (one object per line), returning the result as a [[DataFrame]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json() instead.", "1.4.0")
  def jsonFile(path: String): DataFrame = {
    read.json(path)
  }

  /**
   * Loads a JSON file (one object per line) and applies the given schema,
   * returning the result as a [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json() instead.", "1.4.0")
  def jsonFile(path: String, schema: StructType): DataFrame = {
    read.schema(schema).json(path)
  }

  /**
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json() instead.", "1.4.0")
  def jsonFile(path: String, samplingRatio: Double): DataFrame = {
    read.option("samplingRatio", samplingRatio.toString).json(path)
  }

  /**
   * Loads an RDD[String] storing JSON objects (one object per record), returning the result as a
   * [[DataFrame]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json() instead.", "1.4.0")
  def jsonRDD(json: RDD[String]): DataFrame = read.json(json)

  /**
   * Loads an RDD[String] storing JSON objects (one object per record), returning the result as a
   * [[DataFrame]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json() instead.", "1.4.0")
  def jsonRDD(json: JavaRDD[String]): DataFrame = read.json(json)

  /**
   * Loads an RDD[String] storing JSON objects (one object per record) and applies the given schema,
   * returning the result as a [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json() instead.", "1.4.0")
  def jsonRDD(json: RDD[String], schema: StructType): DataFrame = {
    read.schema(schema).json(json)
  }

  /**
   * Loads an JavaRDD<String> storing JSON objects (one object per record) and applies the given
   * schema, returning the result as a [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json() instead.", "1.4.0")
  def jsonRDD(json: JavaRDD[String], schema: StructType): DataFrame = {
    read.schema(schema).json(json)
  }

  /**
   * Loads an RDD[String] storing JSON objects (one object per record) inferring the
   * schema, returning the result as a [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json() instead.", "1.4.0")
  def jsonRDD(json: RDD[String], samplingRatio: Double): DataFrame = {
    read.option("samplingRatio", samplingRatio.toString).json(json)
  }

  /**
   * Loads a JavaRDD[String] storing JSON objects (one object per record) inferring the
   * schema, returning the result as a [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json() instead.", "1.4.0")
  def jsonRDD(json: JavaRDD[String], samplingRatio: Double): DataFrame = {
    read.option("samplingRatio", samplingRatio.toString).json(json)
  }

  /**
   * Returns the dataset stored at path as a DataFrame,
   * using the default data source configured by spark.sql.sources.default.
   *
   * @group genericdata
   * @deprecated As of 1.4.0, replaced by `read().load(path)`.
   */
  @deprecated("Use read.load(path) instead.", "1.4.0")
  def load(path: String): DataFrame = {
    read.load(path)
  }

  /**
   * Returns the dataset stored at path as a DataFrame, using the given data source.
   *
   * @group genericdata
   * @deprecated As of 1.4.0, replaced by `read().format(source).load(path)`.
   */
  @deprecated("Use read.format(source).load(path) instead.", "1.4.0")
  def load(path: String, source: String): DataFrame = {
    read.format(source).load(path)
  }

  /**
   * (Java-specific) Returns the dataset specified by the given data source and
   * a set of options as a DataFrame.
   *
   * @group genericdata
   * @deprecated As of 1.4.0, replaced by `read().format(source).options(options).load()`.
   */
  @deprecated("Use read.format(source).options(options).load() instead.", "1.4.0")
  def load(source: String, options: java.util.Map[String, String]): DataFrame = {
    read.options(options).format(source).load()
  }

  /**
   * (Scala-specific) Returns the dataset specified by the given data source and
   * a set of options as a DataFrame.
   *
   * @group genericdata
   * @deprecated As of 1.4.0, replaced by `read().format(source).options(options).load()`.
   */
  @deprecated("Use read.format(source).options(options).load() instead.", "1.4.0")
  def load(source: String, options: Map[String, String]): DataFrame = {
    read.options(options).format(source).load()
  }

  /**
   * (Java-specific) Returns the dataset specified by the given data source and
   * a set of options as a DataFrame, using the given schema as the schema of the DataFrame.
   *
   * @group genericdata
   * @deprecated As of 1.4.0, replaced by
   *            `read().format(source).schema(schema).options(options).load()`.
   */
  @deprecated("Use read.format(source).schema(schema).options(options).load() instead.", "1.4.0")
  def load(
      source: String,
      schema: StructType,
      options: java.util.Map[String, String]): DataFrame = {
    read.format(source).schema(schema).options(options).load()
  }

  /**
   * (Scala-specific) Returns the dataset specified by the given data source and
   * a set of options as a DataFrame, using the given schema as the schema of the DataFrame.
   *
   * @group genericdata
   * @deprecated As of 1.4.0, replaced by
   *            `read().format(source).schema(schema).options(options).load()`.
   */
  @deprecated("Use read.format(source).schema(schema).options(options).load() instead.", "1.4.0")
  def load(source: String, schema: StructType, options: Map[String, String]): DataFrame = {
    read.format(source).schema(schema).options(options).load()
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table.
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().jdbc()`.
   */
  @deprecated("Use read.jdbc() instead.", "1.4.0")
  def jdbc(url: String, table: String): DataFrame = {
    read.jdbc(url, table, new Properties)
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table.  Partitions of the table will be retrieved in parallel based on the parameters
   * passed to this function.
   *
   * @param columnName the name of a column of integral type that will be used for partitioning.
   * @param lowerBound the minimum value of `columnName` used to decide partition stride
   * @param upperBound the maximum value of `columnName` used to decide partition stride
   * @param numPartitions the number of partitions.  the range `minValue`-`maxValue` will be split
   *                      evenly into this many partitions
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().jdbc()`.
   */
  @deprecated("Use read.jdbc() instead.", "1.4.0")
  def jdbc(
      url: String,
      table: String,
      columnName: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int): DataFrame = {
    read.jdbc(url, table, columnName, lowerBound, upperBound, numPartitions, new Properties)
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table. The theParts parameter gives a list expressions
   * suitable for inclusion in WHERE clauses; each one defines one partition
   * of the [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().jdbc()`.
   */
  @deprecated("Use read.jdbc() instead.", "1.4.0")
  def jdbc(url: String, table: String, theParts: Array[String]): DataFrame = {
    read.jdbc(url, table, theParts, new Properties)
  }
}

/**
 * This SQLContext object contains utility functions to create a singleton SQLContext instance,
 * or to get the created SQLContext instance.
 *
 * It also provides utility functions to support preference for threads in multiple sessions
 * scenario, setActive could set a SQLContext for current thread, which will be returned by
 * getOrCreate instead of the global one.
 */
object SQLContext {

  /**
   * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
   *
   * This function can be used to create a singleton SQLContext object that can be shared across
   * the JVM.
   *
   * If there is an active SQLContext for current thread, it will be returned instead of the global
   * one.
   *
   * @since 1.5.0
   */
  @deprecated("Use SparkSession.builder instead", "2.0.0")
  def getOrCreate(sparkContext: SparkContext): SQLContext = {
    SparkSession.builder().sparkContext(sparkContext).getOrCreate().sqlContext
  }

  /**
   * Changes the SQLContext that will be returned in this thread and its children when
   * SQLContext.getOrCreate() is called. This can be used to ensure that a given thread receives
   * a SQLContext with an isolated session, instead of the global (first created) context.
   *
   * @since 1.6.0
   */
  @deprecated("Use SparkSession.setActiveSession instead", "2.0.0")
  def setActive(sqlContext: SQLContext): Unit = {
    SparkSession.setActiveSession(sqlContext.sparkSession)
  }

  /**
   * Clears the active SQLContext for current thread. Subsequent calls to getOrCreate will
   * return the first created context instead of a thread-local override.
   *
   * @since 1.6.0
   */
  @deprecated("Use SparkSession.clearActiveSession instead", "2.0.0")
  def clearActive(): Unit = {
    SparkSession.clearActiveSession()
  }

  /**
   * Converts an iterator of Java Beans to InternalRow using the provided
   * bean info & schema. This is not related to the singleton, but is a static
   * method for internal use.
   */
  private[sql] def beansToRows(
        data: Iterator[_],
        beanInfo: BeanInfo,
        attrs: Seq[AttributeReference]): Iterator[InternalRow] = {
    val extractors =
      beanInfo.getPropertyDescriptors.filterNot(_.getName == "class").map(_.getReadMethod)
    val methodsToConverts = extractors.zip(attrs).map { case (e, attr) =>
      (e, CatalystTypeConverters.createToCatalystConverter(attr.dataType))
    }
    data.map{ element =>
      new GenericInternalRow(
        methodsToConverts.map { case (e, convert) => convert(e.invoke(element)) }
      ): InternalRow
    }
  }

  /**
   * Extract `spark.sql.*` properties from the conf and return them as a [[Properties]].
   */
  private[sql] def getSQLProperties(sparkConf: SparkConf): Properties = {
    val properties = new Properties
    sparkConf.getAll.foreach { case (key, value) =>
      if (key.startsWith("spark.sql")) {
        properties.setProperty(key, value)
      }
    }
    properties
  }

}
