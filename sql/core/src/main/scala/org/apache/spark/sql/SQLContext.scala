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

import java.util.Properties

import scala.collection.immutable
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation._
import org.apache.spark.api.java.JavaRDD
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
@Stable
class SQLContext private[sql](val sparkSession: SparkSession)
  extends Logging with Serializable {

  self =>

  sparkSession.sparkContext.assertNotStopped()

  // Note: Since Spark 2.0 this class has become a wrapper of SparkSession, where the
  // real functionality resides. This class remains mainly for backward compatibility.
  // TODO: move this logic into SparkSession

  private[sql] def sessionState: SessionState = sparkSession.sessionState
  private[sql] def sharedState: SharedState = sparkSession.sharedState
  private[sql] def conf: SQLConf = sessionState.conf

  def sparkContext: SparkContext = sparkSession.sparkContext

  /**
   * Returns a [[SQLContext]] as new session, with separated SQL configurations, temporary
   * tables, registered functions, but sharing the same `SparkContext`, cached data and
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
  @Evolving
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
  @Unstable
  def experimental: ExperimentalMethods = sparkSession.experimental

  /**
   * Returns a `DataFrame` with no rows or columns.
   *
   * @group basic
   * @since 1.3.0
   */
  def emptyDataFrame: DataFrame = sparkSession.emptyDataFrame

  /**
   * A collection of methods for registering user-defined functions (UDF).
   *
   * The following example registers a Scala closure as UDF:
   * {{{
   *   sqlContext.udf.register("myUDF", (arg1: Int, arg2: String) => arg2 + arg1)
   * }}}
   *
   * The following example registers a UDF in Java:
   * {{{
   *   sqlContext.udf().register("myUDF",
   *       (Integer arg1, String arg2) -> arg2 + arg1,
   *       DataTypes.StringType);
   * }}}
   *
   * @note The user-defined functions must be deterministic. Due to optimization,
   * duplicate invocations may be eliminated or the function may even be invoked more times than
   * it is present in the query.
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
   * common Scala objects into `DataFrame`s.
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
  @Evolving
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
  @Evolving
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
  @Evolving
  def createDataFrame[A <: Product : TypeTag](data: Seq[A]): DataFrame = {
    sparkSession.createDataFrame(data)
  }

  /**
   * Convert a `BaseRelation` created for external data sources into a `DataFrame`.
   *
   * @group dataframes
   * @since 1.3.0
   */
  def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame = {
    sparkSession.baseRelationToDataFrame(baseRelation)
  }

  /**
   * :: DeveloperApi ::
   * Creates a `DataFrame` from an `RDD` containing [[Row]]s using the given schema.
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
  @Evolving
  def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame = {
    sparkSession.createDataFrame(rowRDD, schema)
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
  @Evolving
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
   * Creates a [[Dataset]] from a `java.util.List` of a given type. This method requires an
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
  @Evolving
  def createDataset[T : Encoder](data: java.util.List[T]): Dataset[T] = {
    sparkSession.createDataset(data)
  }

  /**
   * Creates a DataFrame from an RDD[Row]. User can specify whether the input rows should be
   * converted to Catalyst rows.
   */
  private[sql]
  def internalCreateDataFrame(
      catalystRows: RDD[InternalRow],
      schema: StructType,
      isStreaming: Boolean = false) = {
    sparkSession.internalCreateDataFrame(catalystRows, schema, isStreaming)
  }

  /**
   * :: DeveloperApi ::
   * Creates a `DataFrame` from a `JavaRDD` containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided RDD matches
   * the provided schema. Otherwise, there will be runtime exception.
   *
   * @group dataframes
   * @since 1.3.0
   */
  @DeveloperApi
  @Evolving
  def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame = {
    sparkSession.createDataFrame(rowRDD, schema)
  }

  /**
   * :: DeveloperApi ::
   * Creates a `DataFrame` from a `java.util.List` containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided List matches
   * the provided schema. Otherwise, there will be runtime exception.
   *
   * @group dataframes
   * @since 1.6.0
   */
  @DeveloperApi
  @Evolving
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
   * `DataFrame`.
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
   * Returns a `DataStreamReader` that can be used to read streaming data in as a `DataFrame`.
   * {{{
   *   sparkSession.readStream.parquet("/path/to/directory/of/parquet/files")
   *   sparkSession.readStream.schema(schema).json("/path/to/directory/of/json/files")
   * }}}
   *
   * @since 2.0.0
   */
  @Evolving
  def readStream: DataStreamReader = sparkSession.readStream


  /**
   * Creates an external table from the given path and returns the corresponding DataFrame.
   * It will use the default data source configured by spark.sql.sources.default.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @deprecated("use sparkSession.catalog.createTable instead.", "2.2.0")
  def createExternalTable(tableName: String, path: String): DataFrame = {
    sparkSession.catalog.createTable(tableName, path)
  }

  /**
   * Creates an external table from the given path based on a data source
   * and returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @deprecated("use sparkSession.catalog.createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      path: String,
      source: String): DataFrame = {
    sparkSession.catalog.createTable(tableName, path, source)
  }

  /**
   * Creates an external table from the given path based on a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @deprecated("use sparkSession.catalog.createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      options: java.util.Map[String, String]): DataFrame = {
    sparkSession.catalog.createTable(tableName, source, options)
  }

  /**
   * (Scala-specific)
   * Creates an external table from the given path based on a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @deprecated("use sparkSession.catalog.createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      options: Map[String, String]): DataFrame = {
    sparkSession.catalog.createTable(tableName, source, options)
  }

  /**
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @deprecated("use sparkSession.catalog.createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: java.util.Map[String, String]): DataFrame = {
    sparkSession.catalog.createTable(tableName, source, schema, options)
  }

  /**
   * (Scala-specific)
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @deprecated("use sparkSession.catalog.createTable instead.", "2.2.0")
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame = {
    sparkSession.catalog.createTable(tableName, source, schema, options)
  }

  /**
   * Registers the given `DataFrame` as a temporary table in the catalog. Temporary tables exist
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
   * Creates a `DataFrame` with a single `LongType` column named `id`, containing elements
   * in a range from 0 to `end` (exclusive) with step value 1.
   *
   * @since 1.4.1
   * @group dataframe
   */
  @Experimental
  @Evolving
  def range(end: Long): DataFrame = sparkSession.range(end).toDF()

  /**
   * :: Experimental ::
   * Creates a `DataFrame` with a single `LongType` column named `id`, containing elements
   * in a range from `start` to `end` (exclusive) with step value 1.
   *
   * @since 1.4.0
   * @group dataframe
   */
  @Experimental
  @Evolving
  def range(start: Long, end: Long): DataFrame = sparkSession.range(start, end).toDF()

  /**
   * :: Experimental ::
   * Creates a `DataFrame` with a single `LongType` column named `id`, containing elements
   * in a range from `start` to `end` (exclusive) with a step value.
   *
   * @since 2.0.0
   * @group dataframe
   */
  @Experimental
  @Evolving
  def range(start: Long, end: Long, step: Long): DataFrame = {
    sparkSession.range(start, end, step).toDF()
  }

  /**
   * :: Experimental ::
   * Creates a `DataFrame` with a single `LongType` column named `id`, containing elements
   * in an range from `start` to `end` (exclusive) with an step value, with partition number
   * specified.
   *
   * @since 1.4.0
   * @group dataframe
   */
  @Experimental
  @Evolving
  def range(start: Long, end: Long, step: Long, numPartitions: Int): DataFrame = {
    sparkSession.range(start, end, step, numPartitions).toDF()
  }

  /**
   * Executes a SQL query using Spark, returning the result as a `DataFrame`. The dialect that is
   * used for SQL parsing can be configured with 'spark.sql.dialect'.
   *
   * @group basic
   * @since 1.3.0
   */
  def sql(sqlText: String): DataFrame = sparkSession.sql(sqlText)

  /**
   * Returns the specified table as a `DataFrame`.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def table(tableName: String): DataFrame = {
    sparkSession.table(tableName)
  }

  /**
   * Returns a `DataFrame` containing names of existing tables in the current database.
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
   * Returns a `DataFrame` containing names of existing tables in the given database.
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
   * Returns a `StreamingQueryManager` that allows managing all the
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
    tableNames(sparkSession.catalog.currentDatabase)
  }

  /**
   * Returns the names of tables in the given database as an array.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def tableNames(databaseName: String): Array[String] = {
    sessionState.catalog.listTables(databaseName).map(_.table).toArray
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
   * Converts an iterator of Java Beans to InternalRow using the provided
   * bean info & schema. This is not related to the singleton, but is a static
   * method for internal use.
   */
  private[sql] def beansToRows(
      data: Iterator[_],
      beanClass: Class[_],
      attrs: Seq[AttributeReference]): Iterator[InternalRow] = {
    def createStructConverter(cls: Class[_], fieldTypes: Seq[DataType]): Any => InternalRow = {
      val methodConverters =
        JavaTypeInference.getJavaBeanReadableProperties(cls).zip(fieldTypes)
          .map { case (property, fieldType) =>
            val method = property.getReadMethod
            method -> createConverter(method.getReturnType, fieldType)
          }
      value =>
        if (value == null) {
          null
        } else {
          new GenericInternalRow(
            methodConverters.map { case (method, converter) =>
              converter(method.invoke(value))
            })
        }
    }
    def createConverter(cls: Class[_], dataType: DataType): Any => Any = dataType match {
      case struct: StructType => createStructConverter(cls, struct.map(_.dataType))
      case _ => CatalystTypeConverters.createToCatalystConverter(dataType)
    }
    val dataConverter = createStructConverter(beanClass, attrs.map(_.dataType))
    data.map(dataConverter)
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
