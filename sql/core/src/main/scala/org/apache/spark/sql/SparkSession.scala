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

import java.io.Closeable
import java.util.{ServiceLoader, UUID}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import org.apache.spark.{SPARK_VERSION, SparkConf, SparkContext, SparkException, TaskContext}
import org.apache.spark.annotation.{DeveloperApi, Experimental, Stable, Unstable}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{CALL_SITE_LONG_FORM, CLASS_NAME}
import org.apache.spark.internal.config.{ConfigEntry, EXECUTOR_ALLOW_SPARK_CONTEXT}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession.SPARK_SESSION_UUID_PROPERTY_KEY
import org.apache.spark.sql.artifact.ArtifactManager
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis.{NameParameterizedQuery, PosParameterizedQuery, UnresolvedRelation}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Range}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.ExternalCommandRunner
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.ExternalCommandExecutor
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal._
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.ExecutionListenerManager
import org.apache.spark.util.{CallSite, ThreadUtils, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * The entry point to programming Spark with the Dataset and DataFrame API.
 *
 * In environments that this has been created upfront (e.g. REPL, notebooks), use the builder
 * to get an existing session:
 *
 * {{{
 *   SparkSession.builder().getOrCreate()
 * }}}
 *
 * The builder can also be used to create a new session:
 *
 * {{{
 *   SparkSession.builder
 *     .master("local")
 *     .appName("Word Count")
 *     .config("spark.some.config.option", "some-value")
 *     .getOrCreate()
 * }}}
 *
 * @param sparkContext The Spark context associated with this Spark session.
 * @param existingSharedState If supplied, use the existing shared state
 *                            instead of creating a new one.
 * @param parentSessionState If supplied, inherit all session state (i.e. temporary
 *                            views, SQL config, UDFs etc) from parent.
 */
@Stable
class SparkSession private(
    @transient val sparkContext: SparkContext,
    @transient private val existingSharedState: Option[SharedState],
    @transient private val parentSessionState: Option[SessionState],
    @transient private[sql] val extensions: SparkSessionExtensions,
    @transient private[sql] val initialSessionOptions: Map[String, String],
    @transient private val parentUserDefinedToRealTagsMap: Map[String, String])
  extends Serializable with Closeable with Logging { self =>

  // The call site where this SparkSession was constructed.
  private val creationSite: CallSite = Utils.getCallSite()

  /**
   * Constructor used in Pyspark. Contains explicit application of Spark Session Extensions
   * which otherwise only occurs during getOrCreate. We cannot add this to the default constructor
   * since that would cause every new session to reinvoke Spark Session Extensions on the currently
   * running extensions.
   */
  private[sql] def this(
      sc: SparkContext,
      initialSessionOptions: java.util.HashMap[String, String]) = {
    this(
      sc,
      existingSharedState = None,
      parentSessionState = None,
      SparkSession.applyExtensions(sc, new SparkSessionExtensions),
      initialSessionOptions.asScala.toMap,
      parentUserDefinedToRealTagsMap = Map.empty)
  }

  private[sql] def this(sc: SparkContext) = this(sc, new java.util.HashMap[String, String]())

  private[sql] val sessionUUID: String = UUID.randomUUID.toString

  sparkContext.assertNotStopped()

  // If there is no active SparkSession, uses the default SQL conf. Otherwise, use the session's.
  SQLConf.setSQLConfGetter(() => {
    SparkSession.getActiveSession.filterNot(_.sparkContext.isStopped).map(_.sessionState.conf)
      .getOrElse(SQLConf.getFallbackConf)
  })

  /**
   * A map to hold the mapping from user-defined tags to the real tags attached to Jobs.
   * Real tag have the current session ID attached: `"tag1" -> s"spark-$sessionUUID-tag1"`.
   */
  @transient
  private lazy val userDefinedToRealTagsMap: ConcurrentHashMap[String, String] =
    new ConcurrentHashMap(parentUserDefinedToRealTagsMap.asJava)

  /**
   * The version of Spark on which this application is running.
   *
   * @since 2.0.0
   */
  def version: String = SPARK_VERSION

  /* ----------------------- *
   |  Session-related state  |
   * ----------------------- */

  /**
   * State shared across sessions, including the `SparkContext`, cached data, listener,
   * and a catalog that interacts with external systems.
   *
   * This is internal to Spark and there is no guarantee on interface stability.
   *
   * @since 2.2.0
   */
  @Unstable
  @transient
  lazy val sharedState: SharedState = {
    existingSharedState.getOrElse(new SharedState(sparkContext, initialSessionOptions))
  }

  /**
   * State isolated across sessions, including SQL configurations, temporary tables, registered
   * functions, and everything else that accepts a [[org.apache.spark.sql.internal.SQLConf]].
   * If `parentSessionState` is not null, the `SessionState` will be a copy of the parent.
   *
   * This is internal to Spark and there is no guarantee on interface stability.
   *
   * @since 2.2.0
   */
  @Unstable
  @transient
  lazy val sessionState: SessionState = {
    parentSessionState
      .map(_.clone(this))
      .getOrElse {
        val state = SparkSession.instantiateSessionState(
          SparkSession.sessionStateClassName(sharedState.conf),
          self)
        state
      }
  }

  /**
   * A wrapped version of this session in the form of a [[SQLContext]], for backward compatibility.
   *
   * @since 2.0.0
   */
  @transient
  val sqlContext: SQLContext = new SQLContext(this)

  /**
   * Runtime configuration interface for Spark.
   *
   * This is the interface through which the user can get and set all Spark and Hadoop
   * configurations that are relevant to Spark SQL. When getting the value of a config,
   * this defaults to the value set in the underlying `SparkContext`, if any.
   *
   * @since 2.0.0
   */
  @transient lazy val conf: RuntimeConfig = new RuntimeConfig(sessionState.conf)

  /**
   * An interface to register custom [[org.apache.spark.sql.util.QueryExecutionListener]]s
   * that listen for execution metrics.
   *
   * @since 2.0.0
   */
  def listenerManager: ExecutionListenerManager = sessionState.listenerManager

  /**
   * :: Experimental ::
   * A collection of methods that are considered experimental, but can be used to hook into
   * the query planner for advanced functionality.
   *
   * @since 2.0.0
   */
  @Experimental
  @Unstable
  def experimental: ExperimentalMethods = sessionState.experimentalMethods

  /**
   * A collection of methods for registering user-defined functions (UDF).
   *
   * The following example registers a Scala closure as UDF:
   * {{{
   *   sparkSession.udf.register("myUDF", (arg1: Int, arg2: String) => arg2 + arg1)
   * }}}
   *
   * The following example registers a UDF in Java:
   * {{{
   *   sparkSession.udf().register("myUDF",
   *       (Integer arg1, String arg2) -> arg2 + arg1,
   *       DataTypes.StringType);
   * }}}
   *
   * @note The user-defined functions must be deterministic. Due to optimization,
   * duplicate invocations may be eliminated or the function may even be invoked more times than
   * it is present in the query.
   *
   * @since 2.0.0
   */
  def udf: UDFRegistration = sessionState.udfRegistration

  private[sql] def udtf: UDTFRegistration = sessionState.udtfRegistration

  /**
   * A collection of methods for registering user-defined data sources.
   *
   * @since 4.0.0
   */
  @Experimental
  @Unstable
  def dataSource: DataSourceRegistration = sessionState.dataSourceRegistration

  /**
   * Returns a `StreamingQueryManager` that allows managing all the
   * `StreamingQuery`s active on `this`.
   *
   * @since 2.0.0
   */
  @Unstable
  def streams: StreamingQueryManager = sessionState.streamingQueryManager

  /**
   * Returns an `ArtifactManager` that supports adding, managing and using session-scoped artifacts
   * (jars, classfiles, etc).
   *
   * @since 4.0.0
   */
  @Experimental
  @Unstable
  private[sql] def artifactManager: ArtifactManager = sessionState.artifactManager

  /**
   * Start a new session with isolated SQL configurations, temporary tables, registered
   * functions are isolated, but sharing the underlying `SparkContext` and cached data.
   *
   * @note Other than the `SparkContext`, all shared state is initialized lazily.
   * This method will force the initialization of the shared state to ensure that parent
   * and child sessions are set up with the same shared state. If the underlying catalog
   * implementation is Hive, this will initialize the metastore, which may take some time.
   *
   * @since 2.0.0
   */
  def newSession(): SparkSession = {
    new SparkSession(
      sparkContext,
      Some(sharedState),
      parentSessionState = None,
      extensions,
      initialSessionOptions,
      parentUserDefinedToRealTagsMap = Map.empty)
  }

  /**
   * Create an identical copy of this `SparkSession`, sharing the underlying `SparkContext`
   * and shared state. All the state of this session (i.e. SQL configurations, temporary tables,
   * registered functions) is copied over, and the cloned session is set up with the same shared
   * state as this session. The cloned session is independent of this session, that is, any
   * non-global change in either session is not reflected in the other.
   *
   * @note Other than the `SparkContext`, all shared state is initialized lazily.
   * This method will force the initialization of the shared state to ensure that parent
   * and child sessions are set up with the same shared state. If the underlying catalog
   * implementation is Hive, this will initialize the metastore, which may take some time.
   */
  private[sql] def cloneSession(): SparkSession = {
    val result = new SparkSession(
      sparkContext,
      Some(sharedState),
      Some(sessionState),
      extensions,
      Map.empty,
      userDefinedToRealTagsMap.asScala.toMap)
    result.sessionState // force copy of SessionState
    result.userDefinedToRealTagsMap // force copy of userDefinedToRealTagsMap
    result
  }


  /* --------------------------------- *
   |  Methods for creating DataFrames  |
   * --------------------------------- */

  /**
   * Returns a `DataFrame` with no rows or columns.
   *
   * @since 2.0.0
   */
  @transient
  lazy val emptyDataFrame: DataFrame = Dataset.ofRows(self, LocalRelation())

  /**
   * Creates a new [[Dataset]] of type T containing zero elements.
   *
   * @since 2.0.0
   */
  def emptyDataset[T: Encoder]: Dataset[T] = {
    val encoder = implicitly[Encoder[T]]
    new Dataset(self, LocalRelation(encoder.schema), encoder)
  }

  /**
   * Creates a `DataFrame` from an RDD of Product (e.g. case classes, tuples).
   *
   * @since 2.0.0
   */
  def createDataFrame[A <: Product : TypeTag](rdd: RDD[A]): DataFrame = withActive {
    val encoder = Encoders.product[A]
    Dataset.ofRows(self, ExternalRDD(rdd, self)(encoder))
  }

  /**
   * Creates a `DataFrame` from a local Seq of Product.
   *
   * @since 2.0.0
   */
  def createDataFrame[A <: Product : TypeTag](data: Seq[A]): DataFrame = withActive {
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = toAttributes(schema)
    Dataset.ofRows(self, LocalRelation.fromProduct(attributeSeq, data))
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
   *  val sparkSession = new org.apache.spark.sql.SparkSession(sc)
   *
   *  val schema =
   *    StructType(
   *      StructField("name", StringType, false) ::
   *      StructField("age", IntegerType, true) :: Nil)
   *
   *  val people =
   *    sc.textFile("examples/src/main/resources/people.txt").map(
   *      _.split(",")).map(p => Row(p(0), p(1).trim.toInt))
   *  val dataFrame = sparkSession.createDataFrame(people, schema)
   *  dataFrame.printSchema
   *  // root
   *  // |-- name: string (nullable = false)
   *  // |-- age: integer (nullable = true)
   *
   *  dataFrame.createOrReplaceTempView("people")
   *  sparkSession.sql("select name from people").collect.foreach(println)
   * }}}
   *
   * @since 2.0.0
   */
  @DeveloperApi
  def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame = withActive {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(schema).asInstanceOf[StructType]
    // TODO: use MutableProjection when rowRDD is another DataFrame and the applied
    // schema differs from the existing schema on any field data type.
    val encoder = ExpressionEncoder(replaced)
    val toRow = encoder.createSerializer()
    val catalystRows = rowRDD.map(toRow)
    internalCreateDataFrame(catalystRows.setName(rowRDD.name), schema)
  }

  /**
   * :: DeveloperApi ::
   * Creates a `DataFrame` from a `JavaRDD` containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided RDD matches
   * the provided schema. Otherwise, there will be runtime exception.
   *
   * @since 2.0.0
   */
  @DeveloperApi
  def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(schema).asInstanceOf[StructType]
    createDataFrame(rowRDD.rdd, replaced)
  }

  /**
   * :: DeveloperApi ::
   * Creates a `DataFrame` from a `java.util.List` containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided List matches
   * the provided schema. Otherwise, there will be runtime exception.
   *
   * @since 2.0.0
   */
  @DeveloperApi
  def createDataFrame(rows: java.util.List[Row], schema: StructType): DataFrame = withActive {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(schema).asInstanceOf[StructType]
    Dataset.ofRows(self, LocalRelation.fromExternalRows(toAttributes(replaced), rows.asScala.toSeq))
  }

  /**
   * Applies a schema to an RDD of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   * SELECT * queries will return the columns in an undefined order.
   *
   * @since 2.0.0
   */
  def createDataFrame(rdd: RDD[_], beanClass: Class[_]): DataFrame = withActive {
    val attributeSeq: Seq[AttributeReference] = getSchema(beanClass)
    val className = beanClass.getName
    val rowRdd = rdd.mapPartitions { iter =>
    // BeanInfo is not serializable so we must rediscover it remotely for each partition.
      SQLContext.beansToRows(iter, Utils.classForName(className), attributeSeq)
    }
    Dataset.ofRows(self, LogicalRDD(attributeSeq, rowRdd.setName(rdd.name))(self))
  }

  /**
   * Applies a schema to an RDD of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   * SELECT * queries will return the columns in an undefined order.
   *
   * @since 2.0.0
   */
  def createDataFrame(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame = {
    createDataFrame(rdd.rdd, beanClass)
  }

  /**
   * Applies a schema to a List of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   *          SELECT * queries will return the columns in an undefined order.
   * @since 1.6.0
   */
  def createDataFrame(data: java.util.List[_], beanClass: Class[_]): DataFrame = withActive {
    val attrSeq = getSchema(beanClass)
    val rows = SQLContext.beansToRows(data.asScala.iterator, beanClass, attrSeq)
    Dataset.ofRows(self, LocalRelation(attrSeq, rows.toSeq))
  }

  /**
   * Convert a `BaseRelation` created for external data sources into a `DataFrame`.
   *
   * @since 2.0.0
   */
  def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame = {
    Dataset.ofRows(self, LogicalRelation(baseRelation))
  }

  /* ------------------------------- *
   |  Methods for creating DataSets  |
   * ------------------------------- */

  /**
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
   */
  def createDataset[T : Encoder](data: Seq[T]): Dataset[T] = {
    val enc = encoderFor[T]
    val toRow = enc.createSerializer()
    val attributes = toAttributes(enc.schema)
    val encoded = data.map(d => toRow(d).copy())
    val plan = new LocalRelation(attributes, encoded)
    Dataset[T](self, plan)
  }

  /**
   * Creates a [[Dataset]] from an RDD of a given type. This method requires an
   * encoder (to convert a JVM object of type `T` to and from the internal Spark SQL representation)
   * that is generally created automatically through implicits from a `SparkSession`, or can be
   * created explicitly by calling static methods on [[Encoders]].
   *
   * @since 2.0.0
   */
  def createDataset[T : Encoder](data: RDD[T]): Dataset[T] = {
    Dataset[T](self, ExternalRDD(data, self))
  }

  /**
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
   */
  def createDataset[T : Encoder](data: java.util.List[T]): Dataset[T] = {
    createDataset(data.asScala.toSeq)
  }

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements
   * in a range from 0 to `end` (exclusive) with step value 1.
   *
   * @since 2.0.0
   */
  def range(end: Long): Dataset[java.lang.Long] = range(0, end)

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements
   * in a range from `start` to `end` (exclusive) with step value 1.
   *
   * @since 2.0.0
   */
  def range(start: Long, end: Long): Dataset[java.lang.Long] = {
    range(start, end, step = 1, numPartitions = leafNodeDefaultParallelism)
  }

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements
   * in a range from `start` to `end` (exclusive) with a step value.
   *
   * @since 2.0.0
   */
  def range(start: Long, end: Long, step: Long): Dataset[java.lang.Long] = {
    range(start, end, step, numPartitions = leafNodeDefaultParallelism)
  }

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements
   * in a range from `start` to `end` (exclusive) with a step value, with partition number
   * specified.
   *
   * @since 2.0.0
   */
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[java.lang.Long] = {
    new Dataset(self, Range(start, end, step, numPartitions), Encoders.LONG)
  }

  /**
   * Creates a `DataFrame` from an `RDD[InternalRow]`.
   */
  private[sql] def internalCreateDataFrame(
      catalystRows: RDD[InternalRow],
      schema: StructType,
      isStreaming: Boolean = false): DataFrame = {
    // TODO: use MutableProjection when rowRDD is another DataFrame and the applied
    // schema differs from the existing schema on any field data type.
    val logicalPlan = LogicalRDD(
      toAttributes(schema),
      catalystRows,
      isStreaming = isStreaming)(self)
    Dataset.ofRows(self, logicalPlan)
  }


  /* ------------------------- *
   |  Catalog-related methods  |
   * ------------------------- */

  /**
   * Interface through which the user may create, drop, alter or query underlying
   * databases, tables, functions etc.
   *
   * @since 2.0.0
   */
  @transient lazy val catalog: Catalog = new CatalogImpl(self)

  /**
   * Returns the specified table/view as a `DataFrame`. If it's a table, it must support batch
   * reading and the returned DataFrame is the batch scan query plan of this table. If it's a view,
   * the returned DataFrame is simply the query plan of the view, which can either be a batch or
   * streaming query plan.
   *
   * @param tableName is either a qualified or unqualified name that designates a table or view.
   *                  If a database is specified, it identifies the table/view from the database.
   *                  Otherwise, it first attempts to find a temporary view with the given name
   *                  and then match the table/view from the current database.
   *                  Note that, the global temporary view database is also valid here.
   * @since 2.0.0
   */
  def table(tableName: String): DataFrame = {
    read.table(tableName)
  }

  private[sql] def table(tableIdent: TableIdentifier): DataFrame = {
    Dataset.ofRows(self, UnresolvedRelation(tableIdent))
  }

  /* ----------------- *
   |  Everything else  |
   * ----------------- */

  /**
   * Executes a SQL query substituting positional parameters by the given arguments,
   * returning the result as a `DataFrame`.
   * This API eagerly runs DDL/DML commands, but not for SELECT queries.
   *
   * @param sqlText A SQL statement with positional parameters to execute.
   * @param args An array of Java/Scala objects that can be converted to
   *             SQL literal expressions. See
   *             <a href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html">
   *             Supported Data Types</a> for supported value types in Scala/Java.
   *             For example, 1, "Steven", LocalDate.of(2023, 4, 2).
   *             A value can be also a `Column` of a literal or collection constructor functions
   *             such as `map()`, `array()`, `struct()`, in that case it is taken as is.
   * @param tracker A tracker that can notify when query is ready for execution
   */
  private[sql] def sql(sqlText: String, args: Array[_], tracker: QueryPlanningTracker): DataFrame =
    withActive {
      val plan = tracker.measurePhase(QueryPlanningTracker.PARSING) {
        val parsedPlan = sessionState.sqlParser.parsePlan(sqlText)
        if (args.nonEmpty) {
          PosParameterizedQuery(parsedPlan, args.map(lit(_).expr).toImmutableArraySeq)
        } else {
          parsedPlan
        }
      }
      Dataset.ofRows(self, plan, tracker)
    }

  /**
   * Executes a SQL query substituting positional parameters by the given arguments,
   * returning the result as a `DataFrame`.
   * This API eagerly runs DDL/DML commands, but not for SELECT queries.
   *
   * @param sqlText A SQL statement with positional parameters to execute.
   * @param args An array of Java/Scala objects that can be converted to
   *             SQL literal expressions. See
   *             <a href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html">
   *             Supported Data Types</a> for supported value types in Scala/Java.
   *             For example, 1, "Steven", LocalDate.of(2023, 4, 2).
   *             A value can be also a `Column` of a literal or collection constructor functions
   *             such as `map()`, `array()`, `struct()`, in that case it is taken as is.
   *
   * @since 3.5.0
   */
  @Experimental
  def sql(sqlText: String, args: Array[_]): DataFrame = {
    sql(sqlText, args, new QueryPlanningTracker)
  }

  /**
   * Executes a SQL query substituting named parameters by the given arguments,
   * returning the result as a `DataFrame`.
   * This API eagerly runs DDL/DML commands, but not for SELECT queries.
   *
   * @param sqlText A SQL statement with named parameters to execute.
   * @param args A map of parameter names to Java/Scala objects that can be converted to
   *             SQL literal expressions. See
   *             <a href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html">
   *             Supported Data Types</a> for supported value types in Scala/Java.
   *             For example, map keys: "rank", "name", "birthdate";
   *             map values: 1, "Steven", LocalDate.of(2023, 4, 2).
   *             Map value can be also a `Column` of a literal or collection constructor functions
   *             such as `map()`, `array()`, `struct()`, in that case it is taken as is.
   * @param tracker A tracker that can notify when query is ready for execution
   */
  private[sql] def sql(
      sqlText: String,
      args: Map[String, Any],
      tracker: QueryPlanningTracker): DataFrame =
    withActive {
      val plan = tracker.measurePhase(QueryPlanningTracker.PARSING) {
        val parsedPlan = sessionState.sqlParser.parsePlan(sqlText)
        if (args.nonEmpty) {
          NameParameterizedQuery(parsedPlan, args.transform((_, v) => lit(v).expr))
        } else {
          parsedPlan
        }
      }
      Dataset.ofRows(self, plan, tracker)
    }

  /**
   * Executes a SQL query substituting named parameters by the given arguments,
   * returning the result as a `DataFrame`.
   * This API eagerly runs DDL/DML commands, but not for SELECT queries.
   *
   * @param sqlText A SQL statement with named parameters to execute.
   * @param args A map of parameter names to Java/Scala objects that can be converted to
   *             SQL literal expressions. See
   *             <a href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html">
   *             Supported Data Types</a> for supported value types in Scala/Java.
   *             For example, map keys: "rank", "name", "birthdate";
   *             map values: 1, "Steven", LocalDate.of(2023, 4, 2).
   *             Map value can be also a `Column` of a literal or collection constructor functions
   *             such as `map()`, `array()`, `struct()`, in that case it is taken as is.
   *
   * @since 3.4.0
   */
  @Experimental
  def sql(sqlText: String, args: Map[String, Any]): DataFrame = {
    sql(sqlText, args, new QueryPlanningTracker)
  }

  /**
   * Executes a SQL query substituting named parameters by the given arguments,
   * returning the result as a `DataFrame`.
   * This API eagerly runs DDL/DML commands, but not for SELECT queries.
   *
   * @param sqlText A SQL statement with named parameters to execute.
   * @param args A map of parameter names to Java/Scala objects that can be converted to
   *             SQL literal expressions. See
   *             <a href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html">
   *             Supported Data Types</a> for supported value types in Scala/Java.
   *             For example, map keys: "rank", "name", "birthdate";
   *             map values: 1, "Steven", LocalDate.of(2023, 4, 2).
   *             Map value can be also a `Column` of a literal or collection constructor functions
   *             such as `map()`, `array()`, `struct()`, in that case it is taken as is.
   *
   * @since 3.4.0
   */
  @Experimental
  def sql(sqlText: String, args: java.util.Map[String, Any]): DataFrame = {
    sql(sqlText, args.asScala.toMap)
  }

  /**
   * Executes a SQL query using Spark, returning the result as a `DataFrame`.
   * This API eagerly runs DDL/DML commands, but not for SELECT queries.
   *
   * @since 2.0.0
   */
  def sql(sqlText: String): DataFrame = sql(sqlText, Map.empty[String, Any])

  /**
   * Execute an arbitrary string command inside an external execution engine rather than Spark.
   * This could be useful when user wants to execute some commands out of Spark. For
   * example, executing custom DDL/DML command for JDBC, creating index for ElasticSearch,
   * creating cores for Solr and so on.
   *
   * The command will be eagerly executed after this method is called and the returned
   * DataFrame will contain the output of the command(if any).
   *
   * @param runner The class name of the runner that implements `ExternalCommandRunner`.
   * @param command The target command to be executed
   * @param options The options for the runner.
   *
   * @since 3.0.0
   */
  @Unstable
  def executeCommand(runner: String, command: String, options: Map[String, String]): DataFrame = {
    DataSource.lookupDataSource(runner, sessionState.conf) match {
      case source if classOf[ExternalCommandRunner].isAssignableFrom(source) =>
        Dataset.ofRows(self, ExternalCommandExecutor(
          source.getDeclaredConstructor().newInstance()
            .asInstanceOf[ExternalCommandRunner], command, options))

      case _ =>
        throw QueryCompilationErrors.commandExecutionInRunnerUnsupportedError(runner)
    }
  }


  /**
   * Add a tag to be assigned to all the operations started by this thread in this session.
   *
   * Often, a unit of execution in an application consists of multiple Spark executions.
   * Application programmers can use this method to group all those jobs together and give a group
   * tag. The application can use `org.apache.spark.sql.SparkSession.interruptTag` to cancel all
   * running executions with this tag. For example:
   * {{{
   * // In the main thread:
   * spark.addTag("myjobs")
   * spark.range(10).map(i => { Thread.sleep(10); i }).collect()
   *
   * // In a separate thread:
   * spark.interruptTag("myjobs")
   * }}}
   *
   * There may be multiple tags present at the same time, so different parts of application may
   * use different tags to perform cancellation at different levels of granularity.
   *
   * @param tag
   *   The tag to be added. Cannot contain ',' (comma) character or be an empty string.
   *
   * @since 4.0.0
   */
  def addTag(tag: String): Unit = {
    SparkContext.throwIfInvalidTag(tag)
    userDefinedToRealTagsMap.put(tag, s"spark-session-$sessionUUID-$tag")
  }

  /**
   * Remove a tag previously added to be assigned to all the operations started by this thread in
   * this session. Noop if such a tag was not added earlier.
   *
   * @param tag
   *   The tag to be removed. Cannot contain ',' (comma) character or be an empty string.
   *
   * @since 4.0.0
   */
  def removeTag(tag: String): Unit = {
    SparkContext.throwIfInvalidTag(tag)
    userDefinedToRealTagsMap.remove(tag)
  }

  /**
   * Get the tags that are currently set to be assigned to all the operations started by this
   * thread.
   *
   * @since 4.0.0
   */
  def getTags(): Set[String] = userDefinedToRealTagsMap.keys().asScala.toSet

  /**
   * Clear the current thread's operation tags.
   *
   * @since 4.0.0
   */
  def clearTags(): Unit = userDefinedToRealTagsMap.clear()

  /**
   * Request to interrupt all currently running operations of this session.
   *
   * @note This method will wait up to 60 seconds for the interruption request to be issued.

   * @return Sequence of job IDs requested to be interrupted.

   * @since 4.0.0
   */
  def interruptAll(): Seq[String] = {
    val cancelledIds = sparkContext.cancelAllJobs(
      _.properties.getProperty(SPARK_SESSION_UUID_PROPERTY_KEY) == sessionUUID)
    ThreadUtils.awaitResult(cancelledIds, 60.seconds).map(_.toString).toSeq
  }

  /**
   * Request to interrupt all currently running operations of this session with the given operation
   * tag.
   *
   * @note This method will wait up to 60 seconds for the interruption request to be issued.
   *
   * @return Sequence of job IDs requested to be interrupted.
   */
  def interruptTag(tag: String): Seq[String] = {
    val realTag = userDefinedToRealTagsMap.get(tag)
    if (realTag == null) return Seq.empty

    val cancelledIds = sparkContext.cancelJobsWithTag(realTag, "Interrupted by user", _ => true)
    ThreadUtils.awaitResult(cancelledIds, 60.seconds).map(_.toString).toSeq
  }

  /**
   * Request to interrupt an operation of this session, given its job ID.
   *
   * @note This method will wait up to 60 seconds for the interruption request to be issued.
   *
   * @return The job ID requested to be interrupted, as a single-element sequence, or an empty
   *    sequence if the operation is not started by this session.
   *
   * @since 4.0.0
   */
  def interruptOperation(jobId: String): Seq[String] = {
    scala.util.Try(jobId.toInt).toOption match {
      case Some(jobIdToBeCancelled) =>
        val cancelledIds = sparkContext.cancelJob(
          jobIdToBeCancelled,
          "Interrupted by user",
          shouldCancelJob = _.properties.getProperty(SPARK_SESSION_UUID_PROPERTY_KEY) == sessionUUID
        )
        ThreadUtils.awaitResult(cancelledIds, 60.seconds).map(_.toString).toSeq
      case None =>
        throw new IllegalArgumentException("jobId must be a number in string form.")
    }
  }

  /**
   * Returns a [[DataFrameReader]] that can be used to read non-streaming data in as a
   * `DataFrame`.
   * {{{
   *   sparkSession.read.parquet("/path/to/file.parquet")
   *   sparkSession.read.schema(schema).json("/path/to/file.json")
   * }}}
   *
   * @since 2.0.0
   */
  def read: DataFrameReader = new DataFrameReader(self)

  /**
   * Returns a `DataStreamReader` that can be used to read streaming data in as a `DataFrame`.
   * {{{
   *   sparkSession.readStream.parquet("/path/to/directory/of/parquet/files")
   *   sparkSession.readStream.schema(schema).json("/path/to/directory/of/json/files")
   * }}}
   *
   * @since 2.0.0
   */
  def readStream: DataStreamReader = new DataStreamReader(self)

  /**
   * Executes some code block and prints to stdout the time taken to execute the block. This is
   * available in Scala only and is used primarily for interactive testing and debugging.
   *
   * @since 2.1.0
   */
  def time[T](f: => T): T = {
    val start = System.nanoTime()
    val ret = f
    val end = System.nanoTime()
    // scalastyle:off println
    println(s"Time taken: ${NANOSECONDS.toMillis(end - start)} ms")
    // scalastyle:on println
    ret
  }

  // scalastyle:off
  // Disable style checker so "implicits" object can start with lowercase i
  /**
   * (Scala-specific) Implicit methods available in Scala for converting
   * common Scala objects into `DataFrame`s.
   *
   * {{{
   *   val sparkSession = SparkSession.builder.getOrCreate()
   *   import sparkSession.implicits._
   * }}}
   *
   * @since 2.0.0
   */
  object implicits extends SQLImplicits with Serializable {
    protected override def session: SparkSession = SparkSession.this
  }
  // scalastyle:on

  /**
   * Stop the underlying `SparkContext`.
   *
   * @since 2.0.0
   */
  def stop(): Unit = {
    sparkContext.stop()
  }

  /**
   * Synonym for `stop()`.
   *
   * @since 2.1.0
   */
  override def close(): Unit = stop()

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
  private[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schemaString: String): DataFrame = {
    val schema = DataType.fromJson(schemaString).asInstanceOf[StructType]
    applySchemaToPythonRDD(rdd, schema)
  }

  /**
   * Apply `schema` to an RDD.
   *
   * @note Used by PySpark only
   */
  private[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schema: StructType): DataFrame = {
    val rowRdd = rdd.mapPartitions { iter =>
      val fromJava = python.EvaluatePython.makeFromJava(schema)
      iter.map(r => fromJava(r).asInstanceOf[InternalRow])
    }
    internalCreateDataFrame(rowRdd, schema)
  }

  /**
   * Returns a Catalyst Schema for the given java bean class.
   */
  private def getSchema(beanClass: Class[_]): Seq[AttributeReference] = {
    val (dataType, _) = JavaTypeInference.inferDataType(beanClass)
    dataType.asInstanceOf[StructType].fields.map { f =>
      AttributeReference(f.name, f.dataType, f.nullable)()
    }.toImmutableArraySeq
  }

  /**
   * Execute a block of code with this session set as the active session, and restore the
   * previous session on completion.
   */
  private[sql] def withActive[T](block: => T): T = {
    // Use the active session thread local directly to make sure we get the session that is actually
    // set and not the default session. This to prevent that we promote the default session to the
    // active session once we are done.
    val old = SparkSession.activeThreadSession.get()
    SparkSession.setActiveSession(this)
    try block finally {
      SparkSession.setActiveSession(old)
    }
  }

  private[sql] def leafNodeDefaultParallelism: Int = {
    conf.get(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM).getOrElse(sparkContext.defaultParallelism)
  }

  private[sql] object Converter extends ColumnNodeToExpressionConverter with Serializable {
    override protected def parser: ParserInterface = sessionState.sqlParser
    override protected def conf: SQLConf = sessionState.conf
  }

  private[sql] def expression(e: Column): Expression = Converter(e.node)

  private[sql] implicit class RichColumn(val column: Column) {
    /**
     * Returns the expression for this column.
     */
    def expr: Expression = Converter(column.node)
    /**
     * Returns the expression for this column either with an existing or auto assigned name.
     */
    def named: NamedExpression = ExpressionUtils.toNamed(expr)
  }
}


@Stable
object SparkSession extends Logging {

  private val SPARK_SESSION_UUID_PROPERTY_KEY = "spark.sparkSession.uuid"

  /**
   * Builder for [[SparkSession]].
   */
  @Stable
  class Builder extends Logging {

    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    private[this] val extensions = new SparkSessionExtensions

    private[this] var userSuppliedContext: Option[SparkContext] = None

    private[spark] def sparkContext(sparkContext: SparkContext): Builder = synchronized {
      userSuppliedContext = Option(sparkContext)
      this
    }

    /**
     * Sets a name for the application, which will be shown in the Spark web UI.
     * If no application name is set, a randomly generated name will be used.
     *
     * @since 2.0.0
     */
    def appName(name: String): Builder = config("spark.app.name", name)

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: String): Builder = synchronized {
      options += key -> value
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: Long): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: Double): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: Boolean): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 3.4.0
     */
    def config(map: Map[String, Any]): Builder = synchronized {
      map.foreach {
        kv: (String, Any) => {
          options += kv._1 -> kv._2.toString
        }
      }
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 3.4.0
     */
    def config(map: java.util.Map[String, Any]): Builder = synchronized {
      config(map.asScala.toMap)
    }

    /**
     * Sets a list of config options based on the given `SparkConf`.
     *
     * @since 2.0.0
     */
    def config(conf: SparkConf): Builder = synchronized {
      conf.getAll.foreach { case (k, v) => options += k -> v }
      this
    }

    /**
     * Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to
     * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
     *
     * @since 2.0.0
     */
    def master(master: String): Builder = config("spark.master", master)

    /**
     * Enables Hive support, including connectivity to a persistent Hive metastore, support for
     * Hive serdes, and Hive user-defined functions.
     *
     * @since 2.0.0
     */
    def enableHiveSupport(): Builder = synchronized {
      if (hiveClassesArePresent) {
        config(CATALOG_IMPLEMENTATION.key, "hive")
      } else {
        throw new IllegalArgumentException(
          "Unable to instantiate SparkSession with Hive support because " +
            "Hive classes are not found.")
      }
    }

    /**
     * Inject extensions into the [[SparkSession]]. This allows a user to add Analyzer rules,
     * Optimizer rules, Planning Strategies or a customized parser.
     *
     * @since 2.2.0
     */
    def withExtensions(f: SparkSessionExtensions => Unit): Builder = synchronized {
      f(extensions)
      this
    }

    /**
     * Gets an existing [[SparkSession]] or, if there is no existing one, creates a new
     * one based on the options set in this builder.
     *
     * This method first checks whether there is a valid thread-local SparkSession,
     * and if yes, return that one. It then checks whether there is a valid global
     * default SparkSession, and if yes, return that one. If no valid global default
     * SparkSession exists, the method creates a new SparkSession and assigns the
     * newly created SparkSession as the global default.
     *
     * In case an existing SparkSession is returned, the non-static config options specified in
     * this builder will be applied to the existing SparkSession.
     *
     * @since 2.0.0
     */
    def getOrCreate(): SparkSession = synchronized {
      val sparkConf = new SparkConf()
      options.foreach { case (k, v) => sparkConf.set(k, v) }

      if (!sparkConf.get(EXECUTOR_ALLOW_SPARK_CONTEXT)) {
        assertOnDriver()
      }

      // Get the session from current thread's active session.
      var session = activeThreadSession.get()
      if ((session ne null) && !session.sparkContext.isStopped) {
        applyModifiableSettings(session, new java.util.HashMap[String, String](options.asJava))
        return session
      }

      // Global synchronization so we will only set the default session once.
      SparkSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        session = defaultSession.get()
        if ((session ne null) && !session.sparkContext.isStopped) {
          applyModifiableSettings(session, new java.util.HashMap[String, String](options.asJava))
          return session
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
          // set a random app name if not given.
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(java.util.UUID.randomUUID().toString)
          }

          SparkContext.getOrCreate(sparkConf)
          // Do not update `SparkConf` for existing `SparkContext`, as it's shared by all sessions.
        }

        loadExtensions(extensions)
        applyExtensions(sparkContext, extensions)

        session = new SparkSession(sparkContext,
          existingSharedState = None,
          parentSessionState = None,
          extensions,
          initialSessionOptions = options.toMap,
          parentUserDefinedToRealTagsMap = Map.empty)
        setDefaultSession(session)
        setActiveSession(session)
        registerContextListener(sparkContext)
      }

      return session
    }
  }

  /**
   * Creates a [[SparkSession.Builder]] for constructing a [[SparkSession]].
   *
   * @since 2.0.0
   */
  def builder(): Builder = new Builder

  /**
   * Changes the SparkSession that will be returned in this thread and its children when
   * SparkSession.getOrCreate() is called. This can be used to ensure that a given thread receives
   * a SparkSession with an isolated session, instead of the global (first created) context.
   *
   * @since 2.0.0
   */
  def setActiveSession(session: SparkSession): Unit = {
    clearActiveSession()
    activeThreadSession.set(session)
    if (session != null) {
      session.sparkContext.setLocalProperty(SPARK_SESSION_UUID_PROPERTY_KEY, session.sessionUUID)
      session.userDefinedToRealTagsMap.values().asScala.foreach(session.sparkContext.addJobTag)
    }
  }

  /**
   * Clears the active SparkSession for current thread. Subsequent calls to getOrCreate will
   * return the first created context instead of a thread-local override.
   *
   * @since 2.0.0
   */
  def clearActiveSession(): Unit = {
    getActiveSession match {
      case Some(session) =>
        if (session != null) {
          session.sparkContext.setLocalProperty(SPARK_SESSION_UUID_PROPERTY_KEY, null)
          session.userDefinedToRealTagsMap.values().asScala.foreach(session.sparkContext.addJobTag)
        }
        activeThreadSession.remove()
      case None => // do nothing
    }
  }

  /**
   * Sets the default SparkSession that is returned by the builder.
   *
   * @since 2.0.0
   */
  def setDefaultSession(session: SparkSession): Unit = {
    defaultSession.set(session)
  }

  /**
   * Clears the default SparkSession that is returned by the builder.
   *
   * @since 2.0.0
   */
  def clearDefaultSession(): Unit = {
    defaultSession.set(null)
  }

  /**
   * Returns the active SparkSession for the current thread, returned by the builder.
   *
   * @note Return None, when calling this function on executors
   *
   * @since 2.2.0
   */
  def getActiveSession: Option[SparkSession] = {
    if (Utils.isInRunningSparkTask) {
      // Return None when running on executors.
      None
    } else {
      Option(activeThreadSession.get)
    }
  }

  /**
   * Returns the default SparkSession that is returned by the builder.
   *
   * @note Return None, when calling this function on executors
   *
   * @since 2.2.0
   */
  def getDefaultSession: Option[SparkSession] = {
    if (Utils.isInRunningSparkTask) {
      // Return None when running on executors.
      None
    } else {
      Option(defaultSession.get)
    }
  }

  /**
   * Returns the currently active SparkSession, otherwise the default one. If there is no default
   * SparkSession, throws an exception.
   *
   * @since 2.4.0
   */
  def active: SparkSession = {
    getActiveSession.getOrElse(getDefaultSession.getOrElse(
      throw SparkException.internalError("No active or default Spark session found")))
  }

  /**
   * Apply modifiable settings to an existing [[SparkSession]]. This method are used
   * both in Scala and Python, so put this under [[SparkSession]] object.
   */
  private[sql] def applyModifiableSettings(
      session: SparkSession,
      options: java.util.HashMap[String, String]): Unit = {
    // Lazy val to avoid an unnecessary session state initialization
    lazy val conf = session.sessionState.conf

    val dedupOptions = if (options.isEmpty) Map.empty[String, String] else (
      options.asScala.toSet -- conf.getAllConfs.toSet).toMap
    val (staticConfs, otherConfs) =
      dedupOptions.partition(kv => SQLConf.isStaticConfigKey(kv._1))

    otherConfs.foreach { case (k, v) => conf.setConfString(k, v) }

    // Note that other runtime SQL options, for example, for other third-party datasource
    // can be marked as an ignored configuration here.
    val maybeIgnoredConfs = otherConfs.filterNot { case (k, _) => conf.isModifiable(k) }

    if (staticConfs.nonEmpty || maybeIgnoredConfs.nonEmpty) {
      logWarning(
        "Using an existing Spark session; only runtime SQL configurations will take effect.")
    }
    if (staticConfs.nonEmpty) {
      logDebug("Ignored static SQL configurations:\n  " +
        conf.redactOptions(staticConfs).toSeq.map { case (k, v) => s"$k=$v" }.mkString("\n  "))
    }
    if (maybeIgnoredConfs.nonEmpty) {
      // Only print out non-static and non-runtime SQL configurations.
      // Note that this might show core configurations or source specific
      // options defined in the third-party datasource.
      logDebug("Configurations that might not take effect:\n  " +
        conf.redactOptions(
          maybeIgnoredConfs).toSeq.map { case (k, v) => s"$k=$v" }.mkString("\n  "))
    }
  }

  /**
   * Returns a cloned SparkSession with all specified configurations disabled, or
   * the original SparkSession if all configurations are already disabled.
   */
  private[sql] def getOrCloneSessionWithConfigsOff(
      session: SparkSession,
      configurations: Seq[ConfigEntry[Boolean]]): SparkSession = {
    val configsEnabled = configurations.filter(session.conf.get[Boolean])
    if (configsEnabled.isEmpty) {
      session
    } else {
      val newSession = session.cloneSession()
      configsEnabled.foreach(conf => {
        newSession.conf.set(conf, false)
      })
      newSession
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////////
  // Private methods from now on
  ////////////////////////////////////////////////////////////////////////////////////////

  private val listenerRegistered: AtomicBoolean = new AtomicBoolean(false)

  /** Register the AppEnd listener onto the Context  */
  private def registerContextListener(sparkContext: SparkContext): Unit = {
    if (!listenerRegistered.get()) {
      sparkContext.addSparkListener(new SparkListener {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
          defaultSession.set(null)
          listenerRegistered.set(false)
        }
      })
      listenerRegistered.set(true)
    }
  }

  /** The active SparkSession for the current thread. */
  private val activeThreadSession = new InheritableThreadLocal[SparkSession]

  /** Reference to the root SparkSession. */
  private val defaultSession = new AtomicReference[SparkSession]

  private val HIVE_SESSION_STATE_BUILDER_CLASS_NAME =
    "org.apache.spark.sql.hive.HiveSessionStateBuilder"

  private def sessionStateClassName(conf: SparkConf): String = {
    conf.get(CATALOG_IMPLEMENTATION) match {
      case "hive" => HIVE_SESSION_STATE_BUILDER_CLASS_NAME
      case "in-memory" => classOf[SessionStateBuilder].getCanonicalName
    }
  }

  private def assertOnDriver(): Unit = {
    if (TaskContext.get() != null) {
      // we're accessing it during task execution, fail.
      throw SparkException.internalError(
        "SparkSession should only be created and accessed on the driver.")
    }
  }

  /**
   * Helper method to create an instance of `SessionState` based on `className` from conf.
   * The result is either `SessionState` or a Hive based `SessionState`.
   */
  private def instantiateSessionState(
      className: String,
      sparkSession: SparkSession): SessionState = {
    try {
      // invoke new [Hive]SessionStateBuilder(
      //   SparkSession,
      //   Option[SessionState])
      val clazz = Utils.classForName(className)
      val ctor = clazz.getConstructors.head
      ctor.newInstance(sparkSession, None).asInstanceOf[BaseSessionStateBuilder].build()
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

  /**
   * @return true if Hive classes can be loaded, otherwise false.
   */
  private[spark] def hiveClassesArePresent: Boolean = {
    try {
      Utils.classForName(HIVE_SESSION_STATE_BUILDER_CLASS_NAME)
      Utils.classForName("org.apache.hadoop.hive.conf.HiveConf")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }

  private[spark] def cleanupAnyExistingSession(): Unit = {
    val session = getActiveSession.orElse(getDefaultSession)
    if (session.isDefined) {
      logWarning(
        log"""An existing Spark session exists as the active or default session.
             |This probably means another suite leaked it. Attempting to stop it before continuing.
             |This existing Spark session was created at:
             |
             |${MDC(CALL_SITE_LONG_FORM, session.get.creationSite.longForm)}
             |
           """.stripMargin)
      session.get.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  /**
   * Initialize extensions specified in [[StaticSQLConf]]. The classes will be applied to the
   * extensions passed into this function.
   */
  private def applyExtensions(
      sparkContext: SparkContext,
      extensions: SparkSessionExtensions): SparkSessionExtensions = {
    val extensionConfClassNames = sparkContext.getConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS)
      .getOrElse(Seq.empty)
    extensionConfClassNames.foreach { extensionConfClassName =>
      try {
        val extensionConfClass = Utils.classForName(extensionConfClassName)
        val extensionConf = extensionConfClass.getConstructor().newInstance()
          .asInstanceOf[SparkSessionExtensions => Unit]
        extensionConf(extensions)
      } catch {
        // Ignore the error if we cannot find the class or when the class has the wrong type.
        case e@(_: ClassCastException |
                _: ClassNotFoundException |
                _: NoClassDefFoundError) =>
          logWarning(log"Cannot use ${MDC(CLASS_NAME, extensionConfClassName)} to configure " +
            log"session extensions.", e)
      }
    }
    extensions
  }

  /**
   * Load extensions from [[ServiceLoader]] and use them
   */
  private def loadExtensions(extensions: SparkSessionExtensions): Unit = {
    val loader = ServiceLoader.load(classOf[SparkSessionExtensionsProvider],
      Utils.getContextOrSparkClassLoader)
    val loadedExts = loader.iterator()

    while (loadedExts.hasNext) {
      try {
        val ext = loadedExts.next()
        ext(extensions)
      } catch {
        case e: Throwable => logWarning("Failed to load session extension", e)
      }
    }
  }
}
