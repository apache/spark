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

import java.net.URI
import java.nio.file.Paths
import java.util.{ServiceLoader, UUID}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
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
import org.apache.spark.sql.SparkSession.applyAndLoadExtensions
import org.apache.spark.sql.artifact.ArtifactManager
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis.{NameParameterizedQuery, PosParameterizedQuery, UnresolvedRelation}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{CompoundBody, LocalRelation, LogicalPlan, Range}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.ExternalCommandRunner
import org.apache.spark.sql.errors.{QueryCompilationErrors, SqlScriptingErrors}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.ExternalCommandExecutor
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal._
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.scripting.SqlScriptingExecution
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.ExecutionListenerManager
import org.apache.spark.util.{CallSite, SparkFileUtils, ThreadUtils, Utils}
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
    @transient private val parentManagedJobTags: Map[String, String])
  extends api.SparkSession with Logging with classic.ColumnConversions { self =>

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
      applyAndLoadExtensions(sc), initialSessionOptions.asScala.toMap,
      parentManagedJobTags = Map.empty)
  }

  private[sql] def this(sc: SparkContext) = this(sc, new java.util.HashMap[String, String]())

  private[sql] val sessionUUID: String = UUID.randomUUID.toString

  sparkContext.assertNotStopped()

  // If there is no active SparkSession, uses the default SQL conf. Otherwise, use the session's.
  SQLConf.setSQLConfGetter(() => {
    SparkSession.getActiveSession.filterNot(_.sparkContext.isStopped).map(_.sessionState.conf)
      .getOrElse(SQLConf.getFallbackConf)
  })

  /** Tag to mark all jobs owned by this session. */
  private[sql] lazy val sessionJobTag = s"spark-session-$sessionUUID"

  /**
   * A UUID that is unique on the thread level. Used by managedJobTags to make sure that a same
   * tag from two threads does not overlap in the underlying SparkContext/SQLExecution.
   */
  private[sql] lazy val threadUuid = new InheritableThreadLocal[String] {
    override def childValue(parent: String): String = parent

    override def initialValue(): String = UUID.randomUUID().toString
  }

  /**
   * A map to hold the mapping from user-defined tags to the real tags attached to Jobs.
   * Real tag have the current session ID attached:
   *   tag1 -> spark-session-$sessionUUID-thread-$threadUuid-tag1
   *
   */
  @transient
  private[sql] lazy val managedJobTags = new InheritableThreadLocal[mutable.Map[String, String]] {
      override def childValue(parent: mutable.Map[String, String]): mutable.Map[String, String] = {
        // Note: make a clone such that changes in the parent tags aren't reflected in
        // those of the children threads.
        parent.clone()
      }

      override def initialValue(): mutable.Map[String, String] = {
        mutable.Map(parentManagedJobTags.toSeq: _*)
      }
    }

  /** @inheritdoc */
  def version: String = SPARK_VERSION

  /* ----------------------- *
   |  Session-related state  |
   * ----------------------- */

  /** @inheritdoc */
  @Unstable
  @transient
  lazy val sharedState: SharedState = {
    existingSharedState.getOrElse(new SharedState(sparkContext, initialSessionOptions))
  }

  /** @inheritdoc */
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

  /** @inheritdoc */
  @transient
  val sqlContext: SQLContext = new SQLContext(this)

  /** @inheritdoc */
  @transient lazy val conf: RuntimeConfig = new RuntimeConfigImpl(sessionState.conf)

  /** @inheritdoc */
  def listenerManager: ExecutionListenerManager = sessionState.listenerManager

  /** @inheritdoc */
  @Experimental
  @Unstable
  def experimental: ExperimentalMethods = sessionState.experimentalMethods

  /** @inheritdoc */
  def udf: UDFRegistration = sessionState.udfRegistration

  private[sql] def udtf: UDTFRegistration = sessionState.udtfRegistration

  /**
   * A collection of methods for registering user-defined data sources.
   *
   * @since 4.0.0
   */
  @Experimental
  @Unstable
  private[sql] def dataSource: DataSourceRegistration = sessionState.dataSourceRegistration

  /** @inheritdoc */
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

  /** @inheritdoc */
  def newSession(): SparkSession = {
    new SparkSession(
      sparkContext,
      Some(sharedState),
      parentSessionState = None,
      extensions,
      initialSessionOptions,
      parentManagedJobTags = Map.empty)
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
      managedJobTags.get().toMap)
    result.sessionState // force copy of SessionState
    result.sessionState.artifactManager // force copy of ArtifactManager and its resources
    result.managedJobTags // force copy of managedJobTags
    result
  }


  /* --------------------------------- *
   |  Methods for creating DataFrames  |
   * --------------------------------- */

  /** @inheritdoc */
  @transient
  lazy val emptyDataFrame: DataFrame = Dataset.ofRows(self, LocalRelation())

  /** @inheritdoc */
  def emptyDataset[T: Encoder]: Dataset[T] = {
    val encoder = implicitly[Encoder[T]]
    new Dataset(self, LocalRelation(encoder.schema), encoder)
  }

  /** @inheritdoc */
  def createDataFrame[A <: Product : TypeTag](rdd: RDD[A]): DataFrame = withActive {
    val encoder = Encoders.product[A]
    Dataset.ofRows(self, ExternalRDD(rdd, self)(encoder))
  }

  /** @inheritdoc */
  def createDataFrame[A <: Product : TypeTag](data: Seq[A]): DataFrame = withActive {
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = toAttributes(schema)
    Dataset.ofRows(self, LocalRelation.fromProduct(attributeSeq, data))
  }

  /** @inheritdoc */
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

  /** @inheritdoc */
  @DeveloperApi
  def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame = {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(schema).asInstanceOf[StructType]
    createDataFrame(rowRDD.rdd, replaced)
  }

  /** @inheritdoc */
  @DeveloperApi
  def createDataFrame(rows: java.util.List[Row], schema: StructType): DataFrame = withActive {
    val replaced = CharVarcharUtils.failIfHasCharVarchar(schema).asInstanceOf[StructType]
    Dataset.ofRows(self, LocalRelation.fromExternalRows(toAttributes(replaced), rows.asScala.toSeq))
  }

  /** @inheritdoc */
  def createDataFrame(rdd: RDD[_], beanClass: Class[_]): DataFrame = withActive {
    val attributeSeq: Seq[AttributeReference] = getSchema(beanClass)
    val className = beanClass.getName
    val rowRdd = rdd.mapPartitions { iter =>
    // BeanInfo is not serializable so we must rediscover it remotely for each partition.
      SQLContext.beansToRows(iter, Utils.classForName(className), attributeSeq)
    }
    Dataset.ofRows(self, LogicalRDD(attributeSeq, rowRdd.setName(rdd.name))(self))
  }

  /** @inheritdoc */
  def createDataFrame(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame = {
    createDataFrame(rdd.rdd, beanClass)
  }

  /** @inheritdoc */
  def createDataFrame(data: java.util.List[_], beanClass: Class[_]): DataFrame = withActive {
    val attrSeq = getSchema(beanClass)
    val rows = SQLContext.beansToRows(data.asScala.iterator, beanClass, attrSeq)
    Dataset.ofRows(self, LocalRelation(attrSeq, rows.toSeq))
  }

  /** @inheritdoc */
  def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame = {
    Dataset.ofRows(self, LogicalRelation(baseRelation))
  }

  /* ------------------------------- *
   |  Methods for creating DataSets  |
   * ------------------------------- */

  /** @inheritdoc */
  def createDataset[T : Encoder](data: Seq[T]): Dataset[T] = {
    val enc = encoderFor[T]
    val toRow = enc.createSerializer()
    val attributes = toAttributes(enc.schema)
    val encoded = data.map(d => toRow(d).copy())
    val plan = new LocalRelation(attributes, encoded)
    Dataset[T](self, plan)
  }

  /** @inheritdoc */
  def createDataset[T : Encoder](data: RDD[T]): Dataset[T] = {
    Dataset[T](self, ExternalRDD(data, self))
  }

  /** @inheritdoc */
  def createDataset[T : Encoder](data: java.util.List[T]): Dataset[T] = {
    createDataset(data.asScala.toSeq)
  }

  /** @inheritdoc */
  def range(end: Long): Dataset[java.lang.Long] = range(0, end)

  /** @inheritdoc */
  def range(start: Long, end: Long): Dataset[java.lang.Long] = {
    range(start, end, step = 1, numPartitions = leafNodeDefaultParallelism)
  }

  /** @inheritdoc */
  def range(start: Long, end: Long, step: Long): Dataset[java.lang.Long] = {
    range(start, end, step, numPartitions = leafNodeDefaultParallelism)
  }

  /** @inheritdoc */
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

  /** @inheritdoc */
  @transient lazy val catalog: Catalog = new CatalogImpl(self)

  /** @inheritdoc */
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
   * Executes given script and return the result of the last statement.
   * If script contains no queries, an empty `DataFrame` is returned.
   *
   * @param script A SQL script to execute.
   * @param args A map of parameter names to SQL literal expressions.
   *
   * @return The result as a `DataFrame`.
   */
  private def executeSqlScript(
      script: CompoundBody,
      args: Map[String, Expression] = Map.empty): DataFrame = {
    val sse = new SqlScriptingExecution(script, this, args)
    var result: Option[Seq[Row]] = None

    while (sse.hasNext) {
      sse.withErrorHandling {
        val df = sse.next()
        if (sse.hasNext) {
          df.write.format("noop").mode("overwrite").save()
        } else {
          // Collect results from the last DataFrame.
          result = Some(df.collect().toSeq)
        }
      }
    }

    if (result.isEmpty) {
      emptyDataFrame
    } else {
      val attributes = DataTypeUtils.toAttributes(result.get.head.schema)
      Dataset.ofRows(
        self, LocalRelation.fromExternalRows(attributes, result.get))
    }
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
   * @param tracker A tracker that can notify when query is ready for execution
   */
  private[sql] def sql(sqlText: String, args: Array[_], tracker: QueryPlanningTracker): DataFrame =
    withActive {
      val plan = tracker.measurePhase(QueryPlanningTracker.PARSING) {
        val parsedPlan = sessionState.sqlParser.parsePlan(sqlText)
        parsedPlan match {
          case compoundBody: CompoundBody =>
            if (args.nonEmpty) {
              // Positional parameters are not supported for SQL scripting.
              throw SqlScriptingErrors.positionalParametersAreNotSupportedWithSqlScripting()
            }
            compoundBody
          case logicalPlan: LogicalPlan =>
            if (args.nonEmpty) {
              PosParameterizedQuery(logicalPlan, args.map(lit(_).expr).toImmutableArraySeq)
            } else {
              logicalPlan
            }
        }
      }

      plan match {
        case compoundBody: CompoundBody =>
          // Execute the SQL script.
          executeSqlScript(compoundBody)
        case logicalPlan: LogicalPlan =>
          // Execute the standalone SQL statement.
          Dataset.ofRows(self, plan, tracker)
      }
    }

  /** @inheritdoc */
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
        parsedPlan match {
          case compoundBody: CompoundBody =>
            compoundBody
          case logicalPlan: LogicalPlan =>
            if (args.nonEmpty) {
              NameParameterizedQuery(logicalPlan, args.transform((_, v) => lit(v).expr))
            } else {
              logicalPlan
            }
        }
      }

      plan match {
        case compoundBody: CompoundBody =>
          // Execute the SQL script.
          executeSqlScript(compoundBody, args.transform((_, v) => lit(v).expr))
        case logicalPlan: LogicalPlan =>
          // Execute the standalone SQL statement.
          Dataset.ofRows(self, plan, tracker)
      }
    }

  /** @inheritdoc */
  def sql(sqlText: String, args: Map[String, Any]): DataFrame = {
    sql(sqlText, args, new QueryPlanningTracker)
  }

  /** @inheritdoc */
  override def sql(sqlText: String, args: java.util.Map[String, Any]): DataFrame = {
    sql(sqlText, args.asScala.toMap)
  }

  /** @inheritdoc */
  override def sql(sqlText: String): DataFrame = sql(sqlText, Map.empty[String, Any])

  /**
   * @inheritdoc
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

  /** @inheritdoc */
  @Experimental
  override def addArtifact(path: String): Unit = addArtifact(SparkFileUtils.resolveURI(path))

  /** @inheritdoc */
  @Experimental
  override def addArtifact(uri: URI): Unit = {
    artifactManager.addLocalArtifacts(Artifact.parseArtifacts(uri))
  }

  /** @inheritdoc */
  @Experimental
  override def addArtifact(bytes: Array[Byte], target: String): Unit = {
    val targetPath = Paths.get(target)
    val artifact = Artifact.newArtifactFromExtension(
      targetPath.getFileName.toString,
      targetPath,
      new Artifact.InMemory(bytes))
    artifactManager.addLocalArtifacts(artifact :: Nil)
  }

  /** @inheritdoc */
  @Experimental
  override def addArtifact(source: String, target: String): Unit = {
    val targetPath = Paths.get(target)
    val artifact = Artifact.newArtifactFromExtension(
      targetPath.getFileName.toString,
      targetPath,
      new Artifact.LocalFile(Paths.get(source)))
    artifactManager.addLocalArtifacts(artifact :: Nil)
  }

  /** @inheritdoc */
  @Experimental
  @scala.annotation.varargs
  override def addArtifacts(uri: URI*): Unit = {
    artifactManager.addLocalArtifacts(uri.flatMap(Artifact.parseArtifacts))
  }

  /** @inheritdoc */
  override def addTag(tag: String): Unit = {
    SparkContext.throwIfInvalidTag(tag)
    managedJobTags.get().put(tag, s"spark-session-$sessionUUID-thread-${threadUuid.get()}-$tag")
  }

  /** @inheritdoc */
  override def removeTag(tag: String): Unit = managedJobTags.get().remove(tag)

  /** @inheritdoc */
  override def getTags(): Set[String] = managedJobTags.get().keySet.toSet

  /** @inheritdoc */
  override def clearTags(): Unit = managedJobTags.get().clear()

  /**
   * Request to interrupt all currently running SQL operations of this session.
   *
   * @note Only DataFrame/SQL operations started by this session can be interrupted.
   *
   * @note This method will wait up to 60 seconds for the interruption request to be issued.

   * @return Sequence of SQL execution IDs requested to be interrupted.

   * @since 4.0.0
   */
  override def interruptAll(): Seq[String] =
    doInterruptTag(sessionJobTag, "as part of cancellation of all jobs")

  /**
   * Request to interrupt all currently running SQL operations of this session with the given
   * job tag.
   *
   * @note Only DataFrame/SQL operations started by this session can be interrupted.
   *
   * @note This method will wait up to 60 seconds for the interruption request to be issued.
   *
   * @return Sequence of SQL execution IDs requested to be interrupted.

   * @since 4.0.0
   */
  override def interruptTag(tag: String): Seq[String] = {
    val realTag = managedJobTags.get().get(tag)
    realTag.map(doInterruptTag(_, s"part of cancelled job tags $tag")).getOrElse(Seq.empty)
  }

  private def doInterruptTag(tag: String, reason: String): Seq[String] = {
    val cancelledTags =
      sparkContext.cancelJobsWithTagWithFuture(tag, reason)

    ThreadUtils.awaitResult(cancelledTags, 60.seconds)
      .flatMap(job => Option(job.properties.getProperty(SQLExecution.EXECUTION_ROOT_ID_KEY)))
  }

  /**
   * Request to interrupt a SQL operation of this session, given its SQL execution ID.
   *
   * @note Only DataFrame/SQL operations started by this session can be interrupted.
   *
   * @note This method will wait up to 60 seconds for the interruption request to be issued.
   *
   * @return The execution ID requested to be interrupted, as a single-element sequence, or an empty
   *    sequence if the operation is not started by this session.
   *
   * @since 4.0.0
   */
  override def interruptOperation(operationId: String): Seq[String] = {
    scala.util.Try(operationId.toLong).toOption match {
      case Some(executionIdToBeCancelled) =>
        val tagToBeCancelled = SQLExecution.executionIdJobTag(this, executionIdToBeCancelled)
        doInterruptTag(tagToBeCancelled, reason = "")
      case None =>
        throw new IllegalArgumentException("executionId must be a number in string form.")
    }
  }

  /** @inheritdoc */
  def read: DataFrameReader = new DataFrameReader(self)

  /** @inheritdoc */
  def readStream: DataStreamReader = new DataStreamReader(self)

  /** @inheritdoc */
  def tvf: TableValuedFunction = new TableValuedFunction(self)

  // scalastyle:off
  // Disable style checker so "implicits" object can start with lowercase i
  object implicits extends SQLImplicits {
    override protected def session: SparkSession = self
  }
  // scalastyle:on

  /**
   * Stop the underlying `SparkContext`.
   *
   * @since 2.1.0
   */
  override def close(): Unit = {
    sparkContext.stop()
  }

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
    val old = SparkSession.getActiveSession.orNull
    SparkSession.setActiveSession(this)
    try block finally {
      SparkSession.setActiveSession(old)
    }
  }

  private[sql] def leafNodeDefaultParallelism: Int = {
    sessionState.conf.getConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM)
      .getOrElse(sparkContext.defaultParallelism)
  }

  override protected[sql] val converter: ColumnNodeToExpressionConverter =
    new ColumnNodeToExpressionConverter with Serializable {
      override protected def parser: ParserInterface = sessionState.sqlParser
      override protected def conf: SQLConf = sessionState.conf
    }

  private[sql] lazy val observationManager = new ObservationManager(this)

  override private[sql] def isUsable: Boolean = !sparkContext.isStopped
}


@Stable
object SparkSession extends api.BaseSparkSessionCompanion with Logging {
  override private[sql] type Session = SparkSession

  /**
   * Builder for [[SparkSession]].
   */
  @Stable
  class Builder extends api.SparkSessionBuilder {

    private[this] val extensions = new SparkSessionExtensions

    private[this] var userSuppliedContext: Option[SparkContext] = None

    private[spark] def sparkContext(sparkContext: SparkContext): this.type = synchronized {
      userSuppliedContext = Option(sparkContext)
      this
    }

    /** @inheritdoc */
    override def remote(connectionString: String): this.type = this

    /** @inheritdoc */
    override def appName(name: String): this.type = super.appName(name)

    /** @inheritdoc */
    override def config(key: String, value: String): this.type = super.config(key, value)

    /** @inheritdoc */
    override def config(key: String, value: Long): this.type = super.config(key, value)

    /** @inheritdoc */
    override def config(key: String, value: Double): this.type = super.config(key, value)

    /** @inheritdoc */
    override def config(key: String, value: Boolean): this.type = super.config(key, value)

    /** @inheritdoc */
    override def config(map: Map[String, Any]): this.type = super.config(map)

    /** @inheritdoc */
    override def config(map: java.util.Map[String, Any]): this.type = super.config(map)

    /** @inheritdoc */
    override def config(conf: SparkConf): this.type = super.config(conf)

    /** @inheritdoc */
    override def master(master: String): this.type = super.master(master)

    /** @inheritdoc */
    override def enableHiveSupport(): this.type = synchronized {
      if (hiveClassesArePresent) {
        // TODO(SPARK-50244): We now isolate artifacts added by the `ADD JAR` command. This will
        //  break an existing Hive use case (one session adds JARs and another session uses them).
        //  We need to decide whether/how to enable isolation for Hive.
        super.enableHiveSupport()
          .config(SQLConf.ARTIFACTS_SESSION_ISOLATION_ENABLED.key, false)
      } else {
        throw new IllegalArgumentException(
          "Unable to instantiate SparkSession with Hive support because " +
            "Hive classes are not found.")
      }
    }

    /** @inheritdoc */
    override def withExtensions(f: SparkSessionExtensions => Unit): this.type = synchronized {
      f(extensions)
      this
    }

    private def build(forceCreate: Boolean): SparkSession = synchronized {
      val sparkConf = new SparkConf()
      options.foreach { case (k, v) => sparkConf.set(k, v) }

      if (!sparkConf.get(EXECUTOR_ALLOW_SPARK_CONTEXT)) {
        assertOnDriver()
      }

      // Get the session from current thread's active session.
      val active = getActiveSession
      if (!forceCreate && active.isDefined) {
        val session = active.get
        applyModifiableSettings(session, new java.util.HashMap[String, String](options.asJava))
        return session
      }

      // Global synchronization so we will only set the default session once.
      SparkSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        val default = getDefaultSession
        if (!forceCreate && default.isDefined) {
          val session = default.get
          applyModifiableSettings(session, new java.util.HashMap[String, String](options.asJava))
          return session
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
          // Override appName with the submitted appName
          sparkConf.getOption("spark.submit.appName")
            .map(sparkConf.setAppName)
          // set a random app name if not given.
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(java.util.UUID.randomUUID().toString)
          }

          SparkContext.getOrCreate(sparkConf)
          // Do not update `SparkConf` for existing `SparkContext`, as it's shared by all sessions.
        }

        loadExtensions(extensions)
        applyExtensions(sparkContext, extensions)

        val session = new SparkSession(sparkContext,
          existingSharedState = None,
          parentSessionState = None,
          extensions,
          initialSessionOptions = options.toMap,
          parentManagedJobTags = Map.empty)
        setDefaultAndActiveSession(session)
        registerContextListener(sparkContext)
        session
      }
    }

    /** @inheritdoc */
    def getOrCreate(): SparkSession = build(forceCreate = false)

    /** @inheritdoc */
    def create(): SparkSession = build(forceCreate = true)
  }

  /**
   * Creates a [[SparkSession.Builder]] for constructing a [[SparkSession]].
   *
   * @since 2.0.0
   */
  def builder(): Builder = new Builder

  /** @inheritdoc */
  override def getActiveSession: Option[SparkSession] = super.getActiveSession

  /** @inheritdoc */
  override def getDefaultSession: Option[SparkSession] = super.getDefaultSession

  /** @inheritdoc */
  override def active: SparkSession = super.active

  override protected def canUseSession(session: SparkSession): Boolean =
    session.isUsable && !Utils.isInRunningSparkTask

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
    val configsEnabled = configurations.filter(session.sessionState.conf.getConf[Boolean])
    if (configsEnabled.isEmpty) {
      session
    } else {
      val newSession = session.cloneSession()
      configsEnabled.foreach(conf => {
        newSession.sessionState.conf.setConf(conf, false)
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
          clearDefaultSession()
          clearActiveSession()
          listenerRegistered.set(false)
        }
      })
      listenerRegistered.set(true)
    }
  }

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
   * Create new session extensions, initialize with the confs set in [[StaticSQLConf]],
   * and optionally apply the [[SparkSessionExtensionsProvider]] present on the classpath.
   */
  private[sql] def applyAndLoadExtensions(sparkContext: SparkContext): SparkSessionExtensions = {
    val extensions = applyExtensions(sparkContext, new SparkSessionExtensions)
    if (sparkContext.conf.get(StaticSQLConf.LOAD_SESSION_EXTENSIONS_FROM_CLASSPATH)) {
      loadExtensions(extensions)
    }
    extensions
  }

  /**
   * Initialize extensions specified in [[StaticSQLConf]]. The classes will be applied to the
   * extensions passed into this function.
   */
  private def applyExtensions(
      sparkContext: SparkContext,
      extensions: SparkSessionExtensions): SparkSessionExtensions = {
    val extensionConfClassNames = sparkContext.conf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS)
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
