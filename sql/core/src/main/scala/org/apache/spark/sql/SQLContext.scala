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
import scala.util.control.NonFatal

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SQLConf.SQLConfEntry
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.errors.DialectException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{DefaultOptimizer, Optimizer}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.{InternalRow, ParserDialect, _}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.ui.{SQLListener, SQLTab}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ExecutionListenerManager
import org.apache.spark.sql.{execution => sparkexecution}
import org.apache.spark.util.Utils
import org.apache.spark.{SparkContext, SparkException}

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
 * @groupname Ungrouped Support functions for language integrated queries
 *
 * @since 1.0.0
 */
class SQLContext private[sql](
    @transient val sparkContext: SparkContext,
    @transient protected[sql] val cacheManager: CacheManager,
    @transient private[sql] val listener: SQLListener,
    val isRootContext: Boolean)
  extends org.apache.spark.Logging with Serializable {

  self =>

  def this(sparkContext: SparkContext) = {
    this(sparkContext, new CacheManager, SQLContext.createListenerAndUI(sparkContext), true)
  }
  def this(sparkContext: JavaSparkContext) = this(sparkContext.sc)

  // If spark.sql.allowMultipleContexts is true, we will throw an exception if a user
  // wants to create a new root SQLContext (a SLQContext that is not created by newSession).
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

  /**
   * Returns a SQLContext as new session, with separated SQL configurations, temporary tables,
   * registered functions, but sharing the same SparkContext, CacheManager, SQLListener and SQLTab.
   *
   * @since 1.6.0
   */
  def newSession(): SQLContext = {
    new SQLContext(
      sparkContext = sparkContext,
      cacheManager = cacheManager,
      listener = listener,
      isRootContext = false)
  }

  /**
   * @return Spark SQL configuration
   */
  protected[sql] lazy val conf = new SQLConf

  /**
   * Set Spark SQL configuration properties.
   *
   * @group config
   * @since 1.0.0
   */
  def setConf(props: Properties): Unit = conf.setConf(props)

  /** Set the given Spark SQL configuration property. */
  private[sql] def setConf[T](entry: SQLConfEntry[T], value: T): Unit = conf.setConf(entry, value)

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
   * yet, return `defaultValue` in [[SQLConfEntry]].
   */
  private[sql] def getConf[T](entry: SQLConfEntry[T]): T = conf.getConf(entry)

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue`. This is useful when `defaultValue` in SQLConfEntry is not the
   * desired one.
   */
  private[sql] def getConf[T](entry: SQLConfEntry[T], defaultValue: T): T = {
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

  @transient
  lazy val listenerManager: ExecutionListenerManager = new ExecutionListenerManager

  @transient
  protected[sql] lazy val catalog: Catalog = new SimpleCatalog(conf)

  @transient
  protected[sql] lazy val functionRegistry: FunctionRegistry = FunctionRegistry.builtin.copy()

  @transient
  protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules =
        ExtractPythonUDFs ::
        PreInsertCastAndRename ::
        (if (conf.runSQLOnFile) new ResolveDataSource(self) :: Nil else Nil)

      override val extendedCheckRules = Seq(
        datasources.PreWriteCheck(catalog)
      )
    }

  @transient
  protected[sql] lazy val optimizer: Optimizer = DefaultOptimizer

  @transient
  protected[sql] val ddlParser = new DDLParser(sqlParser.parse(_))

  @transient
  protected[sql] val sqlParser = new SparkSQLParser(getSQLDialect().parse(_))

  protected[sql] def getSQLDialect(): ParserDialect = {
    try {
      val clazz = Utils.classForName(dialectClassName)
      clazz.newInstance().asInstanceOf[ParserDialect]
    } catch {
      case NonFatal(e) =>
        // Since we didn't find the available SQL Dialect, it will fail even for SET command:
        // SET spark.sql.dialect=sql; Let's reset as default dialect automatically.
        val dialect = conf.dialect
        // reset the sql dialect
        conf.unsetConf(SQLConf.DIALECT)
        // throw out the exception, and the default sql dialect will take effect for next query.
        throw new DialectException(
          s"""Instantiating dialect '$dialect' failed.
             |Reverting to default dialect '${conf.dialect}'""".stripMargin, e)
    }
  }

  protected[sql] def parseSql(sql: String): LogicalPlan = ddlParser.parse(sql, false)

  protected[sql] def executeSql(sql: String):
    org.apache.spark.sql.execution.QueryExecution = executePlan(parseSql(sql))

  protected[sql] def executePlan(plan: LogicalPlan) =
    new sparkexecution.QueryExecution(this, plan)

  protected[sql] def dialectClassName = if (conf.dialect == "sql") {
    classOf[DefaultParserDialect].getCanonicalName
  } else {
    conf.dialect
  }

  /**
   * Add a jar to SQLContext
   */
  protected[sql] def addJar(path: String): Unit = {
    sparkContext.addJar(path)
  }

  {
    // We extract spark sql settings from SparkContext's conf and put them to
    // Spark SQL's conf.
    // First, we populate the SQLConf (conf). So, we can make sure that other values using
    // those settings in their construction can get the correct settings.
    // For example, metadataHive in HiveContext may need both spark.sql.hive.metastore.version
    // and spark.sql.hive.metastore.jars to get correctly constructed.
    val properties = new Properties
    sparkContext.getConf.getAll.foreach {
      case (key, value) if key.startsWith("spark.sql") => properties.setProperty(key, value)
      case _ =>
    }
    // We directly put those settings to conf to avoid of calling setConf, which may have
    // side-effects. For example, in HiveContext, setConf may cause executionHive and metadataHive
    // get constructed. If we call setConf directly, the constructed metadataHive may have
    // wrong settings, or the construction may fail.
    conf.setConf(properties)
    // After we have populated SQLConf, we call setConf to populate other confs in the subclass
    // (e.g. hiveconf in HiveContext).
    properties.asScala.foreach {
      case (key, value) => setConf(key, value)
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
  val experimental: ExperimentalMethods = new ExperimentalMethods(this)

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
   * TODO move to SQLSession?
   */
  @transient
  val udf: UDFRegistration = new UDFRegistration(this)

  /**
   * Returns true if the table is currently cached in-memory.
   * @group cachemgmt
   * @since 1.3.0
   */
  def isCached(tableName: String): Boolean = {
    cacheManager.lookupCachedData(table(tableName)).nonEmpty
  }

  /**
    * Returns true if the [[Queryable]] is currently cached in-memory.
    * @group cachemgmt
    * @since 1.3.0
    */
  private[sql] def isCached(qName: Queryable): Boolean = {
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

    /**
     * Converts $"col name" into an [[Column]].
     * @since 1.3.0
     */
    // This must live here to preserve binary compatibility with Spark < 1.5.
    implicit class StringToColumn(val sc: StringContext) {
      def $(args: Any*): ColumnName = {
        new ColumnName(sc.s(args: _*))
      }
    }
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
    DataFrame(self, LogicalRDD(attributeSeq, rowRDD)(self))
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
    DataFrame(self, LocalRelation.fromProduct(attributeSeq, data))
  }

  /**
   * Convert a [[BaseRelation]] created for external data sources into a [[DataFrame]].
   *
   * @group dataframes
   * @since 1.3.0
   */
  def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame = {
    DataFrame(this, LogicalRelation(baseRelation))
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
    DataFrame(this, logicalPlan)
  }


  def createDataset[T : Encoder](data: Seq[T]): Dataset[T] = {
    val enc = encoderFor[T]
    val attributes = enc.schema.toAttributes
    val encoded = data.map(d => enc.toRow(d).copy())
    val plan = new LocalRelation(attributes, encoded)

    new Dataset[T](this, plan)
  }

  def createDataset[T : Encoder](data: RDD[T]): Dataset[T] = {
    val enc = encoderFor[T]
    val attributes = enc.schema.toAttributes
    val encoded = data.map(d => enc.toRow(d))
    val plan = LogicalRDD(attributes, encoded)(self)

    new Dataset[T](this, plan)
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
    DataFrame(this, logicalPlan)
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
    DataFrame(self, LocalRelation.fromExternalRows(schema.toAttributes, rows.asScala))
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
    DataFrame(this, LogicalRDD(attributeSeq, rowRdd)(this))
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
    DataFrame(self, LocalRelation(attrSeq, rows.toArray))
  }


  /**
   * :: Experimental ::
   * Returns a [[DataFrameReader]] that can be used to read data in as a [[DataFrame]].
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
    val tableIdent = SqlParser.parseTableIdentifier(tableName)
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
    val tableIdent = SqlParser.parseTableIdentifier(tableName)
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
    catalog.registerTable(TableIdentifier(tableName), df.logicalPlan)
  }

  /**
   * Drops the temporary table with the given table name in the catalog. If the table has been
   * cached/persisted before, it's also unpersisted.
   *
   * @param tableName the name of the table to be unregistered.
   *
   * @group basic
   * @since 1.3.0
   */
  def dropTempTable(tableName: String): Unit = {
    cacheManager.tryUncacheQuery(table(tableName))
    catalog.unregisterTable(TableIdentifier(tableName))
  }

  /**
   * :: Experimental ::
   * Creates a [[DataFrame]] with a single [[LongType]] column named `id`, containing elements
   * in an range from 0 to `end` (exclusive) with step value 1.
   *
   * @since 1.4.1
   * @group dataframe
   */
  @Experimental
  def range(end: Long): DataFrame = range(0, end)

  /**
   * :: Experimental ::
   * Creates a [[DataFrame]] with a single [[LongType]] column named `id`, containing elements
   * in an range from `start` to `end` (exclusive) with step value 1.
   *
   * @since 1.4.0
   * @group dataframe
   */
  @Experimental
  def range(start: Long, end: Long): DataFrame = {
    createDataFrame(
      sparkContext.range(start, end).map(Row(_)),
      StructType(StructField("id", LongType, nullable = false) :: Nil))
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
  def range(start: Long, end: Long, step: Long, numPartitions: Int): DataFrame = {
    createDataFrame(
      sparkContext.range(start, end, step, numPartitions).map(Row(_)),
      StructType(StructField("id", LongType, nullable = false) :: Nil))
  }

  /**
   * Executes a SQL query using Spark, returning the result as a [[DataFrame]]. The dialect that is
   * used for SQL parsing can be configured with 'spark.sql.dialect'.
   *
   * @group basic
   * @since 1.3.0
   */
  def sql(sqlText: String): DataFrame = {
    DataFrame(this, parseSql(sqlText))
  }

  /**
   * Returns the specified table as a [[DataFrame]].
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def table(tableName: String): DataFrame = {
    table(SqlParser.parseTableIdentifier(tableName))
  }

  private def table(tableIdent: TableIdentifier): DataFrame = {
    DataFrame(this, catalog.lookupRelation(tableIdent))
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
    DataFrame(this, ShowTablesCommand(None))
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
    DataFrame(this, ShowTablesCommand(Some(databaseName)))
  }

  /**
   * Returns the names of tables in the current database as an array.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def tableNames(): Array[String] = {
    catalog.getTables(None).map {
      case (tableName, _) => tableName
    }.toArray
  }

  /**
   * Returns the names of tables in the given database as an array.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def tableNames(databaseName: String): Array[String] = {
    catalog.getTables(Some(databaseName)).map {
      case (tableName, _) => tableName
    }.toArray
  }

  @deprecated("use org.apache.spark.sql.SparkPlanner", "1.6.0")
  protected[sql] class SparkPlanner extends sparkexecution.SparkPlanner(this)

  @transient
  protected[sql] val planner: sparkexecution.SparkPlanner = new sparkexecution.SparkPlanner(this)

  @transient
  protected[sql] lazy val emptyResult = sparkContext.parallelize(Seq.empty[InternalRow], 1)

  /**
   * Prepares a planned SparkPlan for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  @transient
  protected[sql] val prepareForExecution = new RuleExecutor[SparkPlan] {
    val batches = Seq(
      Batch("Add exchange", Once, EnsureRequirements(self)),
      Batch("Add row converters", Once, EnsureRowFormats)
    )
  }

  @deprecated("use org.apache.spark.sql.QueryExecution", "1.6.0")
  protected[sql] class QueryExecution(logical: LogicalPlan)
    extends sparkexecution.QueryExecution(this, logical)

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

    val rowRdd = rdd.map(r => EvaluatePython.fromJava(r, schema).asInstanceOf[InternalRow])
    DataFrame(this, LogicalRDD(schema.toAttributes, rowRdd)(self))
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

  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////
  // Deprecated methods
  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////

  /**
   * @deprecated As of 1.3.0, replaced by `createDataFrame()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use createDataFrame. This will be removed in Spark 2.0.", "1.3.0")
  def applySchema(rowRDD: RDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD, schema)
  }

  /**
   * @deprecated As of 1.3.0, replaced by `createDataFrame()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use createDataFrame. This will be removed in Spark 2.0.", "1.3.0")
  def applySchema(rowRDD: JavaRDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD, schema)
  }

  /**
   * @deprecated As of 1.3.0, replaced by `createDataFrame()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use createDataFrame. This will be removed in Spark 2.0.", "1.3.0")
  def applySchema(rdd: RDD[_], beanClass: Class[_]): DataFrame = {
    createDataFrame(rdd, beanClass)
  }

  /**
   * @deprecated As of 1.3.0, replaced by `createDataFrame()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use createDataFrame. This will be removed in Spark 2.0.", "1.3.0")
  def applySchema(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame = {
    createDataFrame(rdd, beanClass)
  }

  /**
   * Loads a Parquet file, returning the result as a [[DataFrame]]. This function returns an empty
   * [[DataFrame]] if no paths are passed in.
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().parquet()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use read.parquet(). This will be removed in Spark 2.0.", "1.4.0")
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
   * @deprecated As of 1.4.0, replaced by `read().json()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use read.json(). This will be removed in Spark 2.0.", "1.4.0")
  def jsonFile(path: String): DataFrame = {
    read.json(path)
  }

  /**
   * Loads a JSON file (one object per line) and applies the given schema,
   * returning the result as a [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use read.json(). This will be removed in Spark 2.0.", "1.4.0")
  def jsonFile(path: String, schema: StructType): DataFrame = {
    read.schema(schema).json(path)
  }

  /**
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use read.json(). This will be removed in Spark 2.0.", "1.4.0")
  def jsonFile(path: String, samplingRatio: Double): DataFrame = {
    read.option("samplingRatio", samplingRatio.toString).json(path)
  }

  /**
   * Loads an RDD[String] storing JSON objects (one object per record), returning the result as a
   * [[DataFrame]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use read.json(). This will be removed in Spark 2.0.", "1.4.0")
  def jsonRDD(json: RDD[String]): DataFrame = read.json(json)

  /**
   * Loads an RDD[String] storing JSON objects (one object per record), returning the result as a
   * [[DataFrame]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use read.json(). This will be removed in Spark 2.0.", "1.4.0")
  def jsonRDD(json: JavaRDD[String]): DataFrame = read.json(json)

  /**
   * Loads an RDD[String] storing JSON objects (one object per record) and applies the given schema,
   * returning the result as a [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use read.json(). This will be removed in Spark 2.0.", "1.4.0")
  def jsonRDD(json: RDD[String], schema: StructType): DataFrame = {
    read.schema(schema).json(json)
  }

  /**
   * Loads an JavaRDD<String> storing JSON objects (one object per record) and applies the given
   * schema, returning the result as a [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use read.json(). This will be removed in Spark 2.0.", "1.4.0")
  def jsonRDD(json: JavaRDD[String], schema: StructType): DataFrame = {
    read.schema(schema).json(json)
  }

  /**
   * Loads an RDD[String] storing JSON objects (one object per record) inferring the
   * schema, returning the result as a [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use read.json(). This will be removed in Spark 2.0.", "1.4.0")
  def jsonRDD(json: RDD[String], samplingRatio: Double): DataFrame = {
    read.option("samplingRatio", samplingRatio.toString).json(json)
  }

  /**
   * Loads a JavaRDD[String] storing JSON objects (one object per record) inferring the
   * schema, returning the result as a [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use read.json(). This will be removed in Spark 2.0.", "1.4.0")
  def jsonRDD(json: JavaRDD[String], samplingRatio: Double): DataFrame = {
    read.option("samplingRatio", samplingRatio.toString).json(json)
  }

  /**
   * Returns the dataset stored at path as a DataFrame,
   * using the default data source configured by spark.sql.sources.default.
   *
   * @group genericdata
   * @deprecated As of 1.4.0, replaced by `read().load(path)`. This will be removed in Spark 2.0.
   */
  @deprecated("Use read.load(path). This will be removed in Spark 2.0.", "1.4.0")
  def load(path: String): DataFrame = {
    read.load(path)
  }

  /**
   * Returns the dataset stored at path as a DataFrame, using the given data source.
   *
   * @group genericdata
   * @deprecated As of 1.4.0, replaced by `read().format(source).load(path)`.
   *             This will be removed in Spark 2.0.
   */
  @deprecated("Use read.format(source).load(path). This will be removed in Spark 2.0.", "1.4.0")
  def load(path: String, source: String): DataFrame = {
    read.format(source).load(path)
  }

  /**
   * (Java-specific) Returns the dataset specified by the given data source and
   * a set of options as a DataFrame.
   *
   * @group genericdata
   * @deprecated As of 1.4.0, replaced by `read().format(source).options(options).load()`.
   *             This will be removed in Spark 2.0.
   */
  @deprecated("Use read.format(source).options(options).load(). " +
    "This will be removed in Spark 2.0.", "1.4.0")
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
  @deprecated("Use read.format(source).options(options).load(). " +
    "This will be removed in Spark 2.0.", "1.4.0")
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
  @deprecated("Use read.format(source).schema(schema).options(options).load(). " +
    "This will be removed in Spark 2.0.", "1.4.0")
  def load(source: String, schema: StructType, options: java.util.Map[String, String]): DataFrame =
  {
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
  @deprecated("Use read.format(source).schema(schema).options(options).load(). " +
    "This will be removed in Spark 2.0.", "1.4.0")
  def load(source: String, schema: StructType, options: Map[String, String]): DataFrame = {
    read.format(source).schema(schema).options(options).load()
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table.
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().jdbc()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use read.jdbc(). This will be removed in Spark 2.0.", "1.4.0")
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
   * @deprecated As of 1.4.0, replaced by `read().jdbc()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use read.jdbc(). This will be removed in Spark 2.0.", "1.4.0")
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
   * @deprecated As of 1.4.0, replaced by `read().jdbc()`. This will be removed in Spark 2.0.
   */
  @deprecated("Use read.jdbc(). This will be removed in Spark 2.0.", "1.4.0")
  def jdbc(url: String, table: String, theParts: Array[String]): DataFrame = {
    read.jdbc(url, table, theParts, new Properties)
  }

  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////
  // End of deprecated methods
  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////


  // Register a succesfully instantiatd context to the singleton. This should be at the end of
  // the class definition so that the singleton is updated only if there is no exception in the
  // construction of the instance.
  sparkContext.addSparkListener(new SparkListener {
    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      SQLContext.clearInstantiatedContext()
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
    val listener = new SQLListener(sc.conf)
    sc.addSparkListener(listener)
    sc.ui.foreach(new SQLTab(listener, _))
    listener
  }
}
