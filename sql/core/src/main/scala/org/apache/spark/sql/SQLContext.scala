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

import java.beans.{BeanInfo, Introspector}
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Range}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.ShowTablesCommand
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.ui.{SQLListener, SQLTab}
import org.apache.spark.sql.internal.{SessionState, SharedState, SQLConf}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ExecutionListenerManager
import org.apache.spark.util.Utils

/**
 * The entry point for working with structured data (rows and columns) in Spark.  Allows the
 * creation of [[DataFrame]] objects as well as the execution of SQL queries.
 *
 * @groupname basic Basic Operations
 * @groupname ddl_ops Persistent Catalog DDL
 * @groupname cachemgmt Cached Table Management
 * @groupname genericdata Generic Data Sources
 * @groupname specificdata Specific Data Sources
 * @groupname config Configuration
 * @groupname dataframes Custom DataFrame Creation
 * @groupname dataset Custom DataFrame Creation
 * @groupname Ungrouped Support functions for language integrated queries
 * @since 1.0.0
 */
class SQLContext private[sql](
    @transient protected[sql] val sharedState: SharedState,
    val isRootContext: Boolean)
  extends Logging with Serializable {

  self =>

  def this(sc: SparkContext) = {
    this(new SharedState(sc), true)
  }

  def this(sparkContext: JavaSparkContext) = this(sparkContext.sc)

  // If spark.sql.allowMultipleContexts is true, we will throw an exception if a user
  // wants to create a new root SQLContext (a SQLContext that is not created by newSession).
  private val allowMultipleContexts =
    sparkContext.conf.getBoolean(
      SQLConf.ALLOW_MULTIPLE_CONTEXTS.key,
      SQLConf.ALLOW_MULTIPLE_CONTEXTS.defaultValue.get)

  // Assert no root SQLContext is running when allowMultipleContexts is false.
  {
    if (!allowMultipleContexts && isRootContext) {
      SQLContext.getInstantiatedContextOption() match {
        case Some(rootSQLContext) =>
          val errMsg = "Only one SQLContext/HiveContext may be running in this JVM. " +
            s"It is recommended to use SQLContext.getOrCreate to get the instantiated " +
            s"SQLContext/HiveContext. To ignore this error, " +
            s"set ${SQLConf.ALLOW_MULTIPLE_CONTEXTS.key} = true in SparkConf."
          throw new SparkException(errMsg)
        case None => // OK
      }
    }
  }

  def sparkContext: SparkContext = sharedState.sparkContext

  protected[sql] def cacheManager: CacheManager = sharedState.cacheManager
  protected[sql] def listener: SQLListener = sharedState.listener
  protected[sql] def externalCatalog: ExternalCatalog = sharedState.externalCatalog

  /**
   * Returns a [[SQLContext]] as new session, with separated SQL configurations, temporary
   * tables, registered functions, but sharing the same [[SparkContext]], cached data and
   * other things.
   *
   * @since 1.6.0
   */
  def newSession(): SQLContext = new SQLContext(sharedState, isRootContext = false)

  /**
   * Per-session state, e.g. configuration, functions, temporary tables etc.
   */
  @transient
  protected[sql] lazy val sessionState: SessionState = new SessionState(self)
  protected[spark] def conf: SQLConf = sessionState.conf

  /**
   * An interface to register custom [[org.apache.spark.sql.util.QueryExecutionListener]]s
   * that listen for execution metrics.
   */
  @Experimental
  def listenerManager: ExecutionListenerManager = sessionState.listenerManager

  /**
   * Set Spark SQL configuration properties.
   *
   * @group config
   * @since 1.0.0
   */
  def setConf(props: Properties): Unit = conf.setConf(props)

  /** Set the given Spark SQL configuration property. */
  private[sql] def setConf[T](entry: ConfigEntry[T], value: T): Unit = conf.setConf(entry, value)

  /**
   * Set the given Spark SQL configuration property.
   *
   * @group config
   * @since 1.0.0
   */
  def setConf(key: String, value: String): Unit = conf.setConfString(key, value)

  /**
   * Return the value of Spark SQL configuration property for the given key.
   *
   * @group config
   * @since 1.0.0
   */
  def getConf(key: String): String = conf.getConfString(key)

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue` in [[ConfigEntry]].
   */
  private[sql] def getConf[T](entry: ConfigEntry[T]): T = conf.getConf(entry)

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue`. This is useful when `defaultValue` in ConfigEntry is not the
   * desired one.
   */
  private[sql] def getConf[T](entry: ConfigEntry[T], defaultValue: T): T = {
    conf.getConf(entry, defaultValue)
  }

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue`.
   *
   * @group config
   * @since 1.0.0
   */
  def getConf(key: String, defaultValue: String): String = conf.getConfString(key, defaultValue)

  /**
   * Return all the configuration properties that have been set (i.e. not the default).
   * This creates a new copy of the config properties in the form of a Map.
   *
   * @group config
   * @since 1.0.0
   */
  def getAllConfs: immutable.Map[String, String] = conf.getAllConfs

  // Extract `spark.sql.*` entries and put it in our SQLConf.
  // Subclasses may additionally set these entries in other confs.
  SQLContext.getSQLProperties(sparkContext.getConf).asScala.foreach { case (k, v) =>
    setConf(k, v)
  }

  protected[sql] def parseSql(sql: String): LogicalPlan = sessionState.sqlParser.parsePlan(sql)

  protected[sql] def executeSql(sql: String): QueryExecution = executePlan(parseSql(sql))

  protected[sql] def executePlan(plan: LogicalPlan) = new QueryExecution(this, plan)

  /**
   * Add a jar to SQLContext
   */
  protected[sql] def addJar(path: String): Unit = {
    sparkContext.addJar(path)
  }

  /** A [[FunctionResourceLoader]] that can be used in SessionCatalog. */
  @transient protected[sql] lazy val functionResourceLoader: FunctionResourceLoader = {
    new FunctionResourceLoader {
      override def loadResource(resource: FunctionResource): Unit = {
        resource.resourceType match {
          case JarResource => addJar(resource.uri)
          case FileResource => sparkContext.addFile(resource.uri)
          case ArchiveResource =>
            throw new AnalysisException(
              "Archive is not allowed to be loaded. If YARN mode is used, " +
                "please use --archives options while calling spark-submit.")
        }
      }
    }
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
  def experimental: ExperimentalMethods = sessionState.experimentalMethods

  /**
   * :: Experimental ::
   * Returns a [[DataFrame]] with no rows or columns.
   *
   * @group basic
   * @since 1.3.0
   */
  @Experimental
  @transient
  lazy val emptyDataFrame: DataFrame = createDataFrame(sparkContext.emptyRDD[Row], StructType(Nil))

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
  def udf: UDFRegistration = sessionState.udf

  /**
   * Returns true if the table is currently cached in-memory.
   * @group cachemgmt
   * @since 1.3.0
   */
  def isCached(tableName: String): Boolean = {
    cacheManager.lookupCachedData(table(tableName)).nonEmpty
  }

  /**
   * Returns true if the [[Dataset]] is currently cached in-memory.
   * @group cachemgmt
   * @since 1.3.0
   */
  private[sql] def isCached(qName: Dataset[_]): Boolean = {
    cacheManager.lookupCachedData(qName).nonEmpty
  }

  /**
   * Caches the specified table in-memory.
   * @group cachemgmt
   * @since 1.3.0
   */
  def cacheTable(tableName: String): Unit = {
    cacheManager.cacheQuery(table(tableName), Some(tableName))
  }

  /**
   * Removes the specified table from the in-memory cache.
   * @group cachemgmt
   * @since 1.3.0
   */
  def uncacheTable(tableName: String): Unit = cacheManager.uncacheQuery(table(tableName))

  /**
   * Removes all cached tables from the in-memory cache.
   * @since 1.3.0
   */
  def clearCache(): Unit = cacheManager.clearCache()

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
  def createDataFrame[A <: Product : TypeTag](rdd: RDD[A]): DataFrame = {
    SQLContext.setActive(self)
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    val rowRDD = RDDConversions.productToRowRdd(rdd, schema.map(_.dataType))
    Dataset.ofRows(self, LogicalRDD(attributeSeq, rowRDD)(self))
  }

  /**
   * :: Experimental ::
   * Creates a DataFrame from a local Seq of Product.
   *
   * @group dataframes
   * @since 1.3.0
   */
  @Experimental
  def createDataFrame[A <: Product : TypeTag](data: Seq[A]): DataFrame = {
    SQLContext.setActive(self)
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    Dataset.ofRows(self, LocalRelation.fromProduct(attributeSeq, data))
  }

  /**
   * Convert a [[BaseRelation]] created for external data sources into a [[DataFrame]].
   *
   * @group dataframes
   * @since 1.3.0
   */
  def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame = {
    Dataset.ofRows(this, LogicalRelation(baseRelation))
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
   *  dataFrame.registerTempTable("people")
   *  sqlContext.sql("select name from people").collect.foreach(println)
   * }}}
   *
   * @group dataframes
   * @since 1.3.0
   */
  @DeveloperApi
  def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD, schema, needsConversion = true)
  }

  /**
   * Creates a DataFrame from an RDD[Row]. User can specify whether the input rows should be
   * converted to Catalyst rows.
   */
  private[sql]
  def createDataFrame(rowRDD: RDD[Row], schema: StructType, needsConversion: Boolean) = {
    // TODO: use MutableProjection when rowRDD is another DataFrame and the applied
    // schema differs from the existing schema on any field data type.
    val catalystRows = if (needsConversion) {
      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      rowRDD.map(converter(_).asInstanceOf[InternalRow])
    } else {
      rowRDD.map{r: Row => InternalRow.fromSeq(r.toSeq)}
    }
    val logicalPlan = LogicalRDD(schema.toAttributes, catalystRows)(self)
    Dataset.ofRows(this, logicalPlan)
  }


  def createDataset[T : Encoder](data: Seq[T]): Dataset[T] = {
    val enc = encoderFor[T]
    val attributes = enc.schema.toAttributes
    val encoded = data.map(d => enc.toRow(d).copy())
    val plan = new LocalRelation(attributes, encoded)

    Dataset[T](this, plan)
  }

  def createDataset[T : Encoder](data: RDD[T]): Dataset[T] = {
    val enc = encoderFor[T]
    val attributes = enc.schema.toAttributes
    val encoded = data.map(d => enc.toRow(d))
    val plan = LogicalRDD(attributes, encoded)(self)

    Dataset[T](this, plan)
  }

  def createDataset[T : Encoder](data: java.util.List[T]): Dataset[T] = {
    createDataset(data.asScala)
  }

  /**
   * Creates a DataFrame from an RDD[Row]. User can specify whether the input rows should be
   * converted to Catalyst rows.
   */
  private[sql]
  def internalCreateDataFrame(catalystRows: RDD[InternalRow], schema: StructType) = {
    // TODO: use MutableProjection when rowRDD is another DataFrame and the applied
    // schema differs from the existing schema on any field data type.
    val logicalPlan = LogicalRDD(schema.toAttributes, catalystRows)(self)
    Dataset.ofRows(this, logicalPlan)
  }

  /**
   * :: DeveloperApi ::
   * Creates a [[DataFrame]] from an [[JavaRDD]] containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided RDD matches
   * the provided schema. Otherwise, there will be runtime exception.
   *
   * @group dataframes
   * @since 1.3.0
   */
  @DeveloperApi
  def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD.rdd, schema)
  }

  /**
   * :: DeveloperApi ::
   * Creates a [[DataFrame]] from an [[java.util.List]] containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided List matches
   * the provided schema. Otherwise, there will be runtime exception.
   *
   * @group dataframes
   * @since 1.6.0
   */
  @DeveloperApi
  def createDataFrame(rows: java.util.List[Row], schema: StructType): DataFrame = {
    Dataset.ofRows(self, LocalRelation.fromExternalRows(schema.toAttributes, rows.asScala))
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
    val attributeSeq: Seq[AttributeReference] = getSchema(beanClass)
    val className = beanClass.getName
    val rowRdd = rdd.mapPartitions { iter =>
      // BeanInfo is not serializable so we must rediscover it remotely for each partition.
      val localBeanInfo = Introspector.getBeanInfo(Utils.classForName(className))
      SQLContext.beansToRows(iter, localBeanInfo, attributeSeq)
    }
    Dataset.ofRows(this, LogicalRDD(attributeSeq, rowRdd)(this))
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
    createDataFrame(rdd.rdd, beanClass)
  }

  /**
   * Applies a schema to an List of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   *          SELECT * queries will return the columns in an undefined order.
   * @group dataframes
   * @since 1.6.0
   */
  def createDataFrame(data: java.util.List[_], beanClass: Class[_]): DataFrame = {
    val attrSeq = getSchema(beanClass)
    val className = beanClass.getName
    val beanInfo = Introspector.getBeanInfo(beanClass)
    val rows = SQLContext.beansToRows(data.asScala.iterator, beanInfo, attrSeq)
    Dataset.ofRows(self, LocalRelation(attrSeq, rows.toSeq))
  }

  /**
   * :: Experimental ::
   * Returns a [[DataFrameReader]] that can be used to read data and streams in as a [[DataFrame]].
   * {{{
   *   sqlContext.read.parquet("/path/to/file.parquet")
   *   sqlContext.read.schema(schema).json("/path/to/file.json")
   * }}}
   *
   * @group genericdata
   * @since 1.4.0
   */
  @Experimental
  def read: DataFrameReader = new DataFrameReader(this)

  /**
   * :: Experimental ::
   * Creates an external table from the given path and returns the corresponding DataFrame.
   * It will use the default data source configured by spark.sql.sources.default.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @Experimental
  def createExternalTable(tableName: String, path: String): DataFrame = {
    val dataSourceName = conf.defaultDataSourceName
    createExternalTable(tableName, path, dataSourceName)
  }

  /**
   * :: Experimental ::
   * Creates an external table from the given path based on a data source
   * and returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      path: String,
      source: String): DataFrame = {
    createExternalTable(tableName, source, Map("path" -> path))
  }

  /**
   * :: Experimental ::
   * Creates an external table from the given path based on a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      options: java.util.Map[String, String]): DataFrame = {
    createExternalTable(tableName, source, options.asScala.toMap)
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Creates an external table from the given path based on a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      options: Map[String, String]): DataFrame = {
    val tableIdent = sessionState.sqlParser.parseTableIdentifier(tableName)
    val cmd =
      CreateTableUsing(
        tableIdent,
        userSpecifiedSchema = None,
        source,
        temporary = false,
        options,
        allowExisting = false,
        managedIfNoPath = false)
    executePlan(cmd).toRdd
    table(tableIdent)
  }

  /**
   * :: Experimental ::
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: java.util.Map[String, String]): DataFrame = {
    createExternalTable(tableName, source, schema, options.asScala.toMap)
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame = {
    val tableIdent = sessionState.sqlParser.parseTableIdentifier(tableName)
    val cmd =
      CreateTableUsing(
        tableIdent,
        userSpecifiedSchema = Some(schema),
        source,
        temporary = false,
        options,
        allowExisting = false,
        managedIfNoPath = false)
    executePlan(cmd).toRdd
    table(tableIdent)
  }

  /**
   * Registers the given [[DataFrame]] as a temporary table in the catalog. Temporary tables exist
   * only during the lifetime of this instance of SQLContext.
   */
  private[sql] def registerDataFrameAsTable(df: DataFrame, tableName: String): Unit = {
    sessionState.catalog.createTempTable(
      sessionState.sqlParser.parseTableIdentifier(tableName).table,
      df.logicalPlan,
      overrideIfExists = true)
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
    cacheManager.tryUncacheQuery(table(tableName))
    sessionState.catalog.dropTable(TableIdentifier(tableName), ignoreIfNotExists = true)
  }

  /**
   * :: Experimental ::
   * Creates a [[Dataset]] with a single [[LongType]] column named `id`, containing elements
   * in an range from 0 to `end` (exclusive) with step value 1.
   *
   * @since 2.0.0
   * @group dataset
   */
  @Experimental
  def range(end: Long): Dataset[java.lang.Long] = range(0, end)

  /**
   * :: Experimental ::
   * Creates a [[Dataset]] with a single [[LongType]] column named `id`, containing elements
   * in an range from `start` to `end` (exclusive) with step value 1.
   *
   * @since 2.0.0
   * @group dataset
   */
  @Experimental
  def range(start: Long, end: Long): Dataset[java.lang.Long] = {
    range(start, end, step = 1, numPartitions = sparkContext.defaultParallelism)
  }

  /**
   * :: Experimental ::
   * Creates a [[Dataset]] with a single [[LongType]] column named `id`, containing elements
   * in an range from `start` to `end` (exclusive) with an step value.
   *
   * @since 2.0.0
   * @group dataset
   */
  @Experimental
  def range(start: Long, end: Long, step: Long): Dataset[java.lang.Long] = {
    range(start, end, step, numPartitions = sparkContext.defaultParallelism)
  }

  /**
   * :: Experimental ::
   * Creates a [[Dataset]] with a single [[LongType]] column named `id`, containing elements
   * in an range from `start` to `end` (exclusive) with an step value, with partition number
   * specified.
   *
   * @since 2.0.0
   * @group dataset
   */
  @Experimental
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[java.lang.Long] = {
    new Dataset(this, Range(start, end, step, numPartitions), Encoders.LONG)
  }

  /**
   * Executes a SQL query using Spark, returning the result as a [[DataFrame]]. The dialect that is
   * used for SQL parsing can be configured with 'spark.sql.dialect'.
   *
   * @group basic
   * @since 1.3.0
   */
  def sql(sqlText: String): DataFrame = {
    Dataset.ofRows(this, parseSql(sqlText))
  }

  /**
   * Executes a SQL query without parsing it, but instead passing it directly to an underlying
   * system to process. This is currently only used for Hive DDLs and will be removed as soon
   * as Spark can parse all supported Hive DDLs itself.
   */
  private[sql] def runNativeSql(sqlText: String): Seq[Row] = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns the specified table as a [[DataFrame]].
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def table(tableName: String): DataFrame = {
    table(sessionState.sqlParser.parseTableIdentifier(tableName))
  }

  private def table(tableIdent: TableIdentifier): DataFrame = {
    Dataset.ofRows(this, sessionState.catalog.lookupRelation(tableIdent))
  }

  /**
   * Returns a [[DataFrame]] containing names of existing tables in the current database.
   * The returned DataFrame has two columns, tableName and isTemporary (a Boolean
   * indicating if a table is a temporary one or not).
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def tables(): DataFrame = {
    Dataset.ofRows(this, ShowTablesCommand(None, None))
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
    Dataset.ofRows(this, ShowTablesCommand(Some(databaseName), None))
  }

  /**
   * Returns a [[ContinuousQueryManager]] that allows managing all the
   * [[org.apache.spark.sql.ContinuousQuery ContinuousQueries]] active on `this` context.
   *
   * @since 2.0.0
   */
  def streams: ContinuousQueryManager = sessionState.continuousQueryManager

  /**
   * Returns the names of tables in the current database as an array.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def tableNames(): Array[String] = {
    tableNames(sessionState.catalog.getCurrentDatabase)
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

  @transient
  protected[sql] lazy val emptyResult = sparkContext.parallelize(Seq.empty[InternalRow], 1)

  /**
   * Parses the data type in our internal string representation. The data type string should
   * have the same format as the one generated by `toString` in scala.
   * It is only used by PySpark.
   */
  protected[sql] def parseDataType(dataTypeString: String): DataType = {
    DataType.fromJson(dataTypeString)
  }

  /**
   * Apply a schema defined by the schemaString to an RDD. It is only used by PySpark.
   */
  protected[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schemaString: String): DataFrame = {
    val schema = parseDataType(schemaString).asInstanceOf[StructType]
    applySchemaToPythonRDD(rdd, schema)
  }

  /**
   * Apply a schema defined by the schema to an RDD. It is only used by PySpark.
   */
  protected[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schema: StructType): DataFrame = {

    val rowRdd = rdd.map(r => python.EvaluatePython.fromJava(r, schema).asInstanceOf[InternalRow])
    Dataset.ofRows(this, LogicalRDD(schema.toAttributes, rowRdd)(self))
  }

  /**
   * Returns a Catalyst Schema for the given java bean class.
   */
  protected def getSchema(beanClass: Class[_]): Seq[AttributeReference] = {
    val (dataType, _) = JavaTypeInference.inferDataType(beanClass)
    dataType.asInstanceOf[StructType].fields.map { f =>
      AttributeReference(f.name, f.dataType, f.nullable)()
    }
  }

  // Register a successfully instantiated context to the singleton. This should be at the end of
  // the class definition so that the singleton is updated only if there is no exception in the
  // construction of the instance.
  sparkContext.addSparkListener(new SparkListener {
    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      SQLContext.clearInstantiatedContext()
      SQLContext.clearSqlListener()
    }
  })

  SQLContext.setInstantiatedContext(self)
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
   * The active SQLContext for the current thread.
   */
  private val activeContext: InheritableThreadLocal[SQLContext] =
    new InheritableThreadLocal[SQLContext]

  /**
   * Reference to the created SQLContext.
   */
  @transient private val instantiatedContext = new AtomicReference[SQLContext]()

  @transient private val sqlListener = new AtomicReference[SQLListener]()

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
  def getOrCreate(sparkContext: SparkContext): SQLContext = {
    val ctx = activeContext.get()
    if (ctx != null && !ctx.sparkContext.isStopped) {
      return ctx
    }

    synchronized {
      val ctx = instantiatedContext.get()
      if (ctx == null || ctx.sparkContext.isStopped) {
        new SQLContext(sparkContext)
      } else {
        ctx
      }
    }
  }

  private[sql] def clearInstantiatedContext(): Unit = {
    instantiatedContext.set(null)
  }

  private[sql] def setInstantiatedContext(sqlContext: SQLContext): Unit = {
    synchronized {
      val ctx = instantiatedContext.get()
      if (ctx == null || ctx.sparkContext.isStopped) {
        instantiatedContext.set(sqlContext)
      }
    }
  }

  private[sql] def getInstantiatedContextOption(): Option[SQLContext] = {
    Option(instantiatedContext.get())
  }

  private[sql] def clearSqlListener(): Unit = {
    sqlListener.set(null)
  }

  /**
   * Changes the SQLContext that will be returned in this thread and its children when
   * SQLContext.getOrCreate() is called. This can be used to ensure that a given thread receives
   * a SQLContext with an isolated session, instead of the global (first created) context.
   *
   * @since 1.6.0
   */
  def setActive(sqlContext: SQLContext): Unit = {
    activeContext.set(sqlContext)
  }

  /**
   * Clears the active SQLContext for current thread. Subsequent calls to getOrCreate will
   * return the first created context instead of a thread-local override.
   *
   * @since 1.6.0
   */
  def clearActive(): Unit = {
    activeContext.remove()
  }

  private[sql] def getActive(): Option[SQLContext] = {
    Option(activeContext.get())
  }

  /**
   * Converts an iterator of Java Beans to InternalRow using the provided
   * bean info & schema. This is not related to the singleton, but is a static
   * method for internal use.
   */
  private def beansToRows(data: Iterator[_], beanInfo: BeanInfo, attrs: Seq[AttributeReference]):
      Iterator[InternalRow] = {
    val extractors =
      beanInfo.getPropertyDescriptors.filterNot(_.getName == "class").map(_.getReadMethod)
    val methodsToConverts = extractors.zip(attrs).map { case (e, attr) =>
      (e, CatalystTypeConverters.createToCatalystConverter(attr.dataType))
    }
    data.map{ element =>
      new GenericInternalRow(
        methodsToConverts.map { case (e, convert) => convert(e.invoke(element)) }.toArray[Any]
      ): InternalRow
    }
  }

  /**
   * Create a SQLListener then add it into SparkContext, and create an SQLTab if there is SparkUI.
   */
  private[sql] def createListenerAndUI(sc: SparkContext): SQLListener = {
    if (sqlListener.get() == null) {
      val listener = new SQLListener(sc.conf)
      if (sqlListener.compareAndSet(null, listener)) {
        sc.addSparkListener(listener)
        sc.ui.foreach(new SQLTab(listener, _))
      }
    }
    sqlListener.get()
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
