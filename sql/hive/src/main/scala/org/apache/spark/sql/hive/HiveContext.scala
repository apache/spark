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

package org.apache.spark.sql.hive

import java.io.File
import java.net.{URL, URLClassLoader}
import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.language.implicitConversions
import scala.concurrent.duration._

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.StatsSetupConst
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.parse.VariableSubstitution
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde2.io.{DateWritable, TimestampWritable}

import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.SQLConf.SQLConfEntry
import org.apache.spark.sql.SQLConf.SQLConfEntry._
import org.apache.spark.sql.catalyst.{SqlParser, TableIdentifier, ParserDialect}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{ExecutedCommand, ExtractPythonUDFs, SetCommand}
import org.apache.spark.sql.execution.datasources.{PreWriteCheck, PreInsertCastAndRename, DataSourceStrategy}
import org.apache.spark.sql.hive.client._
import org.apache.spark.sql.hive.execution.{DescribeHiveTableCommand, HiveNativeCommand}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


/**
 * This is the HiveQL Dialect, this dialect is strongly bind with HiveContext
 */
private[hive] class HiveQLDialect extends ParserDialect {
  override def parse(sqlText: String): LogicalPlan = {
    HiveQl.parseSql(sqlText)
  }
}

/**
 * An instance of the Spark SQL execution engine that integrates with data stored in Hive.
 * Configuration for Hive is read from hive-site.xml on the classpath.
 *
 * @since 1.0.0
 */
class HiveContext(sc: SparkContext) extends SQLContext(sc) with Logging {
  self =>

  import HiveContext._

  logDebug("create HiveContext")

  /**
   * When true, enables an experimental feature where metastore tables that use the parquet SerDe
   * are automatically converted to use the Spark SQL parquet table scan, instead of the Hive
   * SerDe.
   */
  protected[sql] def convertMetastoreParquet: Boolean = getConf(CONVERT_METASTORE_PARQUET)

  /**
   * When true, also tries to merge possibly different but compatible Parquet schemas in different
   * Parquet data files.
   *
   * This configuration is only effective when "spark.sql.hive.convertMetastoreParquet" is true.
   */
  protected[sql] def convertMetastoreParquetWithSchemaMerging: Boolean =
    getConf(CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING)

  /**
   * When true, a table created by a Hive CTAS statement (no USING clause) will be
   * converted to a data source table, using the data source set by spark.sql.sources.default.
   * The table in CTAS statement will be converted when it meets any of the following conditions:
   *   - The CTAS does not specify any of a SerDe (ROW FORMAT SERDE), a File Format (STORED AS), or
   *     a Storage Hanlder (STORED BY), and the value of hive.default.fileformat in hive-site.xml
   *     is either TextFile or SequenceFile.
   *   - The CTAS statement specifies TextFile (STORED AS TEXTFILE) as the file format and no SerDe
   *     is specified (no ROW FORMAT SERDE clause).
   *   - The CTAS statement specifies SequenceFile (STORED AS SEQUENCEFILE) as the file format
   *     and no SerDe is specified (no ROW FORMAT SERDE clause).
   */
  protected[sql] def convertCTAS: Boolean = getConf(CONVERT_CTAS)

  /**
   * The version of the hive client that will be used to communicate with the metastore.  Note that
   * this does not necessarily need to be the same version of Hive that is used internally by
   * Spark SQL for execution.
   */
  protected[hive] def hiveMetastoreVersion: String =
    getConf(HIVE_METASTORE_VERSION, hiveExecutionVersion)

  /**
   * The location of the jars that should be used to instantiate the HiveMetastoreClient.  This
   * property can be one of three options:
   *  - a classpath in the standard format for both hive and hadoop.
   *  - builtin - attempt to discover the jars that were used to load Spark SQL and use those. This
   *              option is only valid when using the execution version of Hive.
   *  - maven - download the correct version of hive on demand from maven.
   */
  protected[hive] def hiveMetastoreJars: String = getConf(HIVE_METASTORE_JARS)

  /**
   * A comma separated list of class prefixes that should be loaded using the classloader that
   * is shared between Spark SQL and a specific version of Hive. An example of classes that should
   * be shared is JDBC drivers that are needed to talk to the metastore. Other classes that need
   * to be shared are those that interact with classes that are already shared.  For example,
   * custom appenders that are used by log4j.
   */
  protected[hive] def hiveMetastoreSharedPrefixes: Seq[String] =
    getConf(HIVE_METASTORE_SHARED_PREFIXES).filterNot(_ == "")

  /**
   * A comma separated list of class prefixes that should explicitly be reloaded for each version
   * of Hive that Spark SQL is communicating with.  For example, Hive UDFs that are declared in a
   * prefix that typically would be shared (i.e. org.apache.spark.*)
   */
  protected[hive] def hiveMetastoreBarrierPrefixes: Seq[String] =
    getConf(HIVE_METASTORE_BARRIER_PREFIXES).filterNot(_ == "")

  /*
   * hive thrift server use background spark sql thread pool to execute sql queries
   */
  protected[hive] def hiveThriftServerAsync: Boolean = getConf(HIVE_THRIFT_SERVER_ASYNC)

  @transient
  protected[sql] lazy val substitutor = new VariableSubstitution()

  /**
   * The copy of the hive client that is used for execution.  Currently this must always be
   * Hive 13 as this is the version of Hive that is packaged with Spark SQL.  This copy of the
   * client is used for execution related tasks like registering temporary functions or ensuring
   * that the ThreadLocal SessionState is correctly populated.  This copy of Hive is *not* used
   * for storing persistent metadata, and only point to a dummy metastore in a temporary directory.
   */
  @transient
  protected[hive] lazy val executionHive: ClientWrapper = {
    logInfo(s"Initializing execution hive, version $hiveExecutionVersion")
    new ClientWrapper(
      version = IsolatedClientLoader.hiveVersion(hiveExecutionVersion),
      config = newTemporaryConfiguration(),
      initClassLoader = Utils.getContextOrSparkClassLoader)
  }
  SessionState.setCurrentSessionState(executionHive.state)

  /**
   * Overrides default Hive configurations to avoid breaking changes to Spark SQL users.
   *  - allow SQL11 keywords to be used as identifiers
   */
  private[sql] def defaultOverrides() = {
    setConf(ConfVars.HIVE_SUPPORT_SQL11_RESERVED_KEYWORDS.varname, "false")
  }

  defaultOverrides()

  /**
   * The copy of the Hive client that is used to retrieve metadata from the Hive MetaStore.
   * The version of the Hive client that is used here must match the metastore that is configured
   * in the hive-site.xml file.
   */
  @transient
  protected[hive] lazy val metadataHive: ClientInterface = {
    val metaVersion = IsolatedClientLoader.hiveVersion(hiveMetastoreVersion)

    // We instantiate a HiveConf here to read in the hive-site.xml file and then pass the options
    // into the isolated client loader
    val metadataConf = new HiveConf()

    val defaultWarehouseLocation = metadataConf.get("hive.metastore.warehouse.dir")
    logInfo("default warehouse location is " + defaultWarehouseLocation)

    // `configure` goes second to override other settings.
    val allConfig = metadataConf.iterator.map(e => e.getKey -> e.getValue).toMap ++ configure

    val isolatedLoader = if (hiveMetastoreJars == "builtin") {
      if (hiveExecutionVersion != hiveMetastoreVersion) {
        throw new IllegalArgumentException(
          "Builtin jars can only be used when hive execution version == hive metastore version. " +
          s"Execution: ${hiveExecutionVersion} != Metastore: ${hiveMetastoreVersion}. " +
          "Specify a vaild path to the correct hive jars using $HIVE_METASTORE_JARS " +
          s"or change $HIVE_METASTORE_VERSION to $hiveExecutionVersion.")
      }

      // We recursively find all jars in the class loader chain,
      // starting from the given classLoader.
      def allJars(classLoader: ClassLoader): Array[URL] = classLoader match {
        case null => Array.empty[URL]
        case urlClassLoader: URLClassLoader =>
          urlClassLoader.getURLs ++ allJars(urlClassLoader.getParent)
        case other => allJars(other.getParent)
      }

      val classLoader = Utils.getContextOrSparkClassLoader
      val jars = allJars(classLoader)
      if (jars.length == 0) {
        throw new IllegalArgumentException(
          "Unable to locate hive jars to connect to metastore. " +
            "Please set spark.sql.hive.metastore.jars.")
      }

      logInfo(
        s"Initializing HiveMetastoreConnection version $hiveMetastoreVersion using Spark classes.")
      new IsolatedClientLoader(
        version = metaVersion,
        execJars = jars.toSeq,
        config = allConfig,
        isolationOn = true,
        barrierPrefixes = hiveMetastoreBarrierPrefixes,
        sharedPrefixes = hiveMetastoreSharedPrefixes)
    } else if (hiveMetastoreJars == "maven") {
      // TODO: Support for loading the jars from an already downloaded location.
      logInfo(
        s"Initializing HiveMetastoreConnection version $hiveMetastoreVersion using maven.")
      IsolatedClientLoader.forVersion(
        version = hiveMetastoreVersion,
        config = allConfig,
        barrierPrefixes = hiveMetastoreBarrierPrefixes,
        sharedPrefixes = hiveMetastoreSharedPrefixes)
    } else {
      // Convert to files and expand any directories.
      val jars =
        hiveMetastoreJars
          .split(File.pathSeparator)
          .flatMap {
            case path if new File(path).getName() == "*" =>
              val files = new File(path).getParentFile().listFiles()
              if (files == null) {
                logWarning(s"Hive jar path '$path' does not exist.")
                Nil
              } else {
                files.filter(_.getName().toLowerCase().endsWith(".jar"))
              }
            case path =>
              new File(path) :: Nil
          }
          .map(_.toURI.toURL)

      logInfo(
        s"Initializing HiveMetastoreConnection version $hiveMetastoreVersion using $jars")
      new IsolatedClientLoader(
        version = metaVersion,
        execJars = jars.toSeq,
        config = allConfig,
        isolationOn = true,
        barrierPrefixes = hiveMetastoreBarrierPrefixes,
        sharedPrefixes = hiveMetastoreSharedPrefixes)
    }
    isolatedLoader.client
  }

  protected[sql] override def parseSql(sql: String): LogicalPlan = {
    var state = SessionState.get()
    if (state == null) {
      SessionState.setCurrentSessionState(tlSession.get().asInstanceOf[SQLSession].sessionState)
    }
    super.parseSql(substitutor.substitute(hiveconf, sql))
  }

  override protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution(plan)

  /**
   * Invalidate and refresh all the cached the metadata of the given table. For performance reasons,
   * Spark SQL or the external data source library it uses might cache certain metadata about a
   * table, such as the location of blocks. When those change outside of Spark SQL, users should
   * call this function to invalidate the cache.
   *
   * @since 1.3.0
   */
  def refreshTable(tableName: String): Unit = {
    val tableIdent = new SqlParser().parseTableIdentifier(tableName)
    catalog.refreshTable(tableIdent)
  }

  protected[hive] def invalidateTable(tableName: String): Unit = {
    val tableIdent = new SqlParser().parseTableIdentifier(tableName)
    catalog.invalidateTable(tableIdent)
  }

  /**
   * Analyzes the given table in the current database to generate statistics, which will be
   * used in query optimizations.
   *
   * Right now, it only supports Hive tables and it only updates the size of a Hive table
   * in the Hive metastore.
   *
   * @since 1.2.0
   */
  @Experimental
  def analyze(tableName: String) {
    val tableIdent = new SqlParser().parseTableIdentifier(tableName)
    val relation = EliminateSubQueries(catalog.lookupRelation(tableIdent.toSeq))

    relation match {
      case relation: MetastoreRelation =>
        // This method is mainly based on
        // org.apache.hadoop.hive.ql.stats.StatsUtils.getFileSizeForTable(HiveConf, Table)
        // in Hive 0.13 (except that we do not use fs.getContentSummary).
        // TODO: Generalize statistics collection.
        // TODO: Why fs.getContentSummary returns wrong size on Jenkins?
        // Can we use fs.getContentSummary in future?
        // Seems fs.getContentSummary returns wrong table size on Jenkins. So we use
        // countFileSize to count the table size.
        val stagingDir = metadataHive.getConf(HiveConf.ConfVars.STAGINGDIR.varname,
          HiveConf.ConfVars.STAGINGDIR.defaultStrVal)

        def calculateTableSize(fs: FileSystem, path: Path): Long = {
          val fileStatus = fs.getFileStatus(path)
          val size = if (fileStatus.isDir) {
            fs.listStatus(path)
              .map { status =>
                if (!status.getPath().getName().startsWith(stagingDir)) {
                  calculateTableSize(fs, status.getPath)
                } else {
                  0L
                }
              }
              .sum
          } else {
            fileStatus.getLen
          }

          size
        }

        def getFileSizeForTable(conf: HiveConf, table: Table): Long = {
          val path = table.getPath
          var size: Long = 0L
          try {
            val fs = path.getFileSystem(conf)
            size = calculateTableSize(fs, path)
          } catch {
            case e: Exception =>
              logWarning(
                s"Failed to get the size of table ${table.getTableName} in the " +
                s"database ${table.getDbName} because of ${e.toString}", e)
              size = 0L
          }

          size
        }

        val tableParameters = relation.hiveQlTable.getParameters
        val oldTotalSize =
          Option(tableParameters.get(StatsSetupConst.TOTAL_SIZE))
            .map(_.toLong)
            .getOrElse(0L)
        val newTotalSize = getFileSizeForTable(hiveconf, relation.hiveQlTable)
        // Update the Hive metastore if the total size of the table is different than the size
        // recorded in the Hive metastore.
        // This logic is based on org.apache.hadoop.hive.ql.exec.StatsTask.aggregateStats().
        if (newTotalSize > 0 && newTotalSize != oldTotalSize) {
          catalog.client.alterTable(
            relation.table.copy(
              properties = relation.table.properties +
                (StatsSetupConst.TOTAL_SIZE -> newTotalSize.toString)))
        }
      case otherRelation =>
        throw new UnsupportedOperationException(
          s"Analyze only works for Hive tables, but $tableName is a ${otherRelation.nodeName}")
    }
  }

  protected[hive] def hiveconf = tlSession.get().asInstanceOf[this.SQLSession].hiveconf

  override def setConf(key: String, value: String): Unit = {
    super.setConf(key, value)
    executionHive.runSqlHive(s"SET $key=$value")
    metadataHive.runSqlHive(s"SET $key=$value")
    // If users put any Spark SQL setting in the spark conf (e.g. spark-defaults.conf),
    // this setConf will be called in the constructor of the SQLContext.
    // Also, calling hiveconf will create a default session containing a HiveConf, which
    // will interfer with the creation of executionHive (which is a lazy val). So,
    // we put hiveconf.set at the end of this method.
    hiveconf.set(key, value)
  }

  override private[sql] def setConf[T](entry: SQLConfEntry[T], value: T): Unit = {
    setConf(entry.key, entry.stringConverter(value))
  }

    /* A catalyst metadata catalog that points to the Hive Metastore. */
  @transient
  override protected[sql] lazy val catalog =
    new HiveMetastoreCatalog(metadataHive, this) with OverrideCatalog

  // Note that HiveUDFs will be overridden by functions registered in this context.
  @transient
  override protected[sql] lazy val functionRegistry: FunctionRegistry =
    new HiveFunctionRegistry(FunctionRegistry.builtin)

  /* An analyzer that uses the Hive metastore. */
  @transient
  override protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules =
        catalog.ParquetConversions ::
        catalog.CreateTables ::
        catalog.PreInsertionCasts ::
        ExtractPythonUDFs ::
        ResolveHiveWindowFunction ::
        PreInsertCastAndRename ::
        Nil

      override val extendedCheckRules = Seq(
        PreWriteCheck(catalog)
      )
    }

  override protected[sql] def createSession(): SQLSession = {
    new this.SQLSession()
  }

  /** Overridden by child classes that need to set configuration before the client init. */
  protected def configure(): Map[String, String] = {
    // Hive 0.14.0 introduces timeout operations in HiveConf, and changes default values of a bunch
    // of time `ConfVar`s by adding time suffixes (`s`, `ms`, and `d` etc.).  This breaks backwards-
    // compatibility when users are trying to connecting to a Hive metastore of lower version,
    // because these options are expected to be integral values in lower versions of Hive.
    //
    // Here we enumerate all time `ConfVar`s and convert their values to numeric strings according
    // to their output time units.
    Seq(
      ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY -> TimeUnit.SECONDS,
      ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.METASTORE_CLIENT_SOCKET_LIFETIME -> TimeUnit.SECONDS,
      ConfVars.HMSHANDLERINTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.METASTORE_EVENT_DB_LISTENER_TTL -> TimeUnit.SECONDS,
      ConfVars.METASTORE_EVENT_CLEAN_FREQ -> TimeUnit.SECONDS,
      ConfVars.METASTORE_EVENT_EXPIRY_DURATION -> TimeUnit.SECONDS,
      ConfVars.METASTORE_AGGREGATE_STATS_CACHE_TTL -> TimeUnit.SECONDS,
      ConfVars.METASTORE_AGGREGATE_STATS_CACHE_MAX_WRITER_WAIT -> TimeUnit.MILLISECONDS,
      ConfVars.METASTORE_AGGREGATE_STATS_CACHE_MAX_READER_WAIT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVES_AUTO_PROGRESS_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_LOG_INCREMENTAL_PLAN_PROGRESS_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_STATS_JDBC_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_STATS_RETRIES_WAIT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES -> TimeUnit.SECONDS,
      ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_TXN_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_COMPACTOR_WORKER_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_COMPACTOR_CHECK_INTERVAL -> TimeUnit.SECONDS,
      ConfVars.HIVE_COMPACTOR_CLEANER_RUN_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_MAX_IDLE_TIME -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_WORKER_KEEPALIVE_TIME -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_MAX_AGE -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_LOGIN_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_WORKER_KEEPALIVE_TIME -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_ASYNC_EXEC_KEEPALIVE_TIME -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_SESSION_CHECK_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_IDLE_SESSION_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_IDLE_OPERATION_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.SERVER_READ_SOCKET_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_LOCALIZE_RESOURCE_WAIT_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.SPARK_CLIENT_FUTURE_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.SPARK_JOB_MONITOR_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.SPARK_RPC_CLIENT_CONNECT_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.SPARK_RPC_CLIENT_HANDSHAKE_TIMEOUT -> TimeUnit.MILLISECONDS
    ).map { case (confVar, unit) =>
      confVar.varname -> hiveconf.getTimeVar(confVar, unit).toString
    }.toMap
  }

  protected[hive] class SQLSession extends super.SQLSession {
    protected[sql] override lazy val conf: SQLConf = new SQLConf {
      override def dialect: String = getConf(SQLConf.DIALECT, "hiveql")
      override def caseSensitiveAnalysis: Boolean = getConf(SQLConf.CASE_SENSITIVE, false)
    }

    /**
     * SQLConf and HiveConf contracts:
     *
     * 1. reuse existing started SessionState if any
     * 2. when the Hive session is first initialized, params in HiveConf will get picked up by the
     *    SQLConf.  Additionally, any properties set by set() or a SET command inside sql() will be
     *    set in the SQLConf *as well as* in the HiveConf.
     */
    protected[hive] lazy val sessionState: SessionState = {
      var state = SessionState.get()
      if (state == null) {
        state = new SessionState(new HiveConf(classOf[SessionState]))
        SessionState.start(state)
      }
      state
    }

    protected[hive] lazy val hiveconf: HiveConf = {
      setConf(sessionState.getConf.getAllProperties)
      sessionState.getConf
    }
  }

  override protected[sql] def dialectClassName = if (conf.dialect == "hiveql") {
    classOf[HiveQLDialect].getCanonicalName
  } else {
    super.dialectClassName
  }

  @transient
  private val hivePlanner = new SparkPlanner with HiveStrategies {
    val hiveContext = self

    override def strategies: Seq[Strategy] = experimental.extraStrategies ++ Seq(
      DataSourceStrategy,
      HiveCommandStrategy(self),
      HiveDDLStrategy,
      DDLStrategy,
      TakeOrderedAndProject,
      InMemoryScans,
      HiveTableScans,
      DataSinks,
      Scripts,
      HashAggregation,
      Aggregation,
      LeftSemiJoin,
      EquiJoinSelection,
      BasicOperators,
      CartesianProduct,
      BroadcastNestedLoopJoin
    )
  }

  protected[hive] def runSqlHive(sql: String): Seq[String] = {
    if (sql.toLowerCase.contains("create temporary function")) {
      executionHive.runSqlHive(sql)
    } else if (sql.trim.toLowerCase.startsWith("set")) {
      metadataHive.runSqlHive(sql)
      executionHive.runSqlHive(sql)
    } else {
      metadataHive.runSqlHive(sql)
    }
  }

  @transient
  override protected[sql] val planner = hivePlanner

  /** Extends QueryExecution with hive specific features. */
  protected[sql] class QueryExecution(logicalPlan: LogicalPlan)
    extends super.QueryExecution(logicalPlan) {

    /**
     * Returns the result as a hive compatible sequence of strings.  For native commands, the
     * execution is simply passed back to Hive.
     */
    def stringResult(): Seq[String] = executedPlan match {
      case ExecutedCommand(desc: DescribeHiveTableCommand) =>
        // If it is a describe command for a Hive table, we want to have the output format
        // be similar with Hive.
        desc.run(self).map {
          case Row(name: String, dataType: String, comment) =>
            Seq(name, dataType,
              Option(comment.asInstanceOf[String]).getOrElse(""))
              .map(s => String.format(s"%-20s", s))
              .mkString("\t")
        }
      case command: ExecutedCommand =>
        command.executeCollect().map(_(0).toString)

      case other =>
        val result: Seq[Seq[Any]] = other.executeCollect().map(_.toSeq).toSeq
        // We need the types so we can output struct field names
        val types = analyzed.output.map(_.dataType)
        // Reformat to match hive tab delimited output.
        result.map(_.zip(types).map(HiveContext.toHiveString)).map(_.mkString("\t")).toSeq
    }

    override def simpleString: String =
      logical match {
        case _: HiveNativeCommand => "<Native command: executed by Hive>"
        case _: SetCommand => "<SET command: executed by Hive, and noted by SQLContext>"
        case _ => super.simpleString
      }
  }
}


private[hive] object HiveContext {
  /** The version of hive used internally by Spark SQL. */
  val hiveExecutionVersion: String = "1.2.1"

  val HIVE_METASTORE_VERSION: String = "spark.sql.hive.metastore.version"
  val HIVE_METASTORE_JARS = stringConf("spark.sql.hive.metastore.jars",
    defaultValue = Some("builtin"),
    doc = s"""
      | Location of the jars that should be used to instantiate the HiveMetastoreClient.
      | This property can be one of three options: "
      | 1. "builtin"
      |   Use Hive ${hiveExecutionVersion}, which is bundled with the Spark assembly jar when
      |   <code>-Phive</code> is enabled. When this option is chosen,
      |   <code>spark.sql.hive.metastore.version</code> must be either
      |   <code>${hiveExecutionVersion}</code> or not defined.
      | 2. "maven"
      |   Use Hive jars of specified version downloaded from Maven repositories.
      | 3. A classpath in the standard format for both Hive and Hadoop.
    """.stripMargin)
  val CONVERT_METASTORE_PARQUET = booleanConf("spark.sql.hive.convertMetastoreParquet",
    defaultValue = Some(true),
    doc = "When set to false, Spark SQL will use the Hive SerDe for parquet tables instead of " +
      "the built in support.")

  val CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING = booleanConf(
    "spark.sql.hive.convertMetastoreParquet.mergeSchema",
    defaultValue = Some(false),
    doc = "TODO")

  val CONVERT_CTAS = booleanConf("spark.sql.hive.convertCTAS",
    defaultValue = Some(false),
    doc = "TODO")

  val HIVE_METASTORE_SHARED_PREFIXES = stringSeqConf("spark.sql.hive.metastore.sharedPrefixes",
    defaultValue = Some(jdbcPrefixes),
    doc = "A comma separated list of class prefixes that should be loaded using the classloader " +
      "that is shared between Spark SQL and a specific version of Hive. An example of classes " +
      "that should be shared is JDBC drivers that are needed to talk to the metastore. Other " +
      "classes that need to be shared are those that interact with classes that are already " +
      "shared. For example, custom appenders that are used by log4j.")

  private def jdbcPrefixes = Seq(
    "com.mysql.jdbc", "org.postgresql", "com.microsoft.sqlserver", "oracle.jdbc")

  val HIVE_METASTORE_BARRIER_PREFIXES = stringSeqConf("spark.sql.hive.metastore.barrierPrefixes",
    defaultValue = Some(Seq()),
    doc = "A comma separated list of class prefixes that should explicitly be reloaded for each " +
      "version of Hive that Spark SQL is communicating with. For example, Hive UDFs that are " +
      "declared in a prefix that typically would be shared (i.e. <code>org.apache.spark.*</code>).")

  val HIVE_THRIFT_SERVER_ASYNC = booleanConf("spark.sql.hive.thriftServer.async",
    defaultValue = Some(true),
    doc = "TODO")

  /** Constructs a configuration for hive, where the metastore is located in a temp directory. */
  def newTemporaryConfiguration(): Map[String, String] = {
    val tempDir = Utils.createTempDir()
    val localMetastore = new File(tempDir, "metastore")
    val propMap: HashMap[String, String] = HashMap()
    // We have to mask all properties in hive-site.xml that relates to metastore data source
    // as we used a local metastore here.
    HiveConf.ConfVars.values().foreach { confvar =>
      if (confvar.varname.contains("datanucleus") || confvar.varname.contains("jdo")) {
        propMap.put(confvar.varname, confvar.getDefaultExpr())
      }
    }
    propMap.put(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, localMetastore.toURI.toString)
    propMap.put(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname,
      s"jdbc:derby:;databaseName=${localMetastore.getAbsolutePath};create=true")
    propMap.put("datanucleus.rdbms.datastoreAdapterClassName",
      "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
    propMap.toMap
  }

  protected val primitiveTypes =
    Seq(StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, ByteType,
      ShortType, DateType, TimestampType, BinaryType)

  protected[sql] def toHiveString(a: (Any, DataType)): String = a match {
    case (struct: Row, StructType(fields)) =>
      struct.toSeq.zip(fields).map {
        case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
      }.mkString("{", ",", "}")
    case (seq: Seq[_], ArrayType(typ, _)) =>
      seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
    case (map: Map[_, _], MapType(kType, vType, _)) =>
      map.map {
        case (key, value) =>
          toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
      }.toSeq.sorted.mkString("{", ",", "}")
    case (null, _) => "NULL"
    case (d: Int, DateType) => new DateWritable(d).toString
    case (t: Timestamp, TimestampType) => new TimestampWritable(t).toString
    case (bin: Array[Byte], BinaryType) => new String(bin, "UTF-8")
    case (decimal: java.math.BigDecimal, DecimalType()) =>
      // Hive strips trailing zeros so use its toString
      HiveDecimal.create(decimal).toString
    case (other, tpe) if primitiveTypes contains tpe => other.toString
  }

  /** Hive outputs fields of structs slightly differently than top level attributes. */
  protected def toHiveStructString(a: (Any, DataType)): String = a match {
    case (struct: Row, StructType(fields)) =>
      struct.toSeq.zip(fields).map {
        case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
      }.mkString("{", ",", "}")
    case (seq: Seq[_], ArrayType(typ, _)) =>
      seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
    case (map: Map[_, _], MapType(kType, vType, _)) =>
      map.map {
        case (key, value) =>
          toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
      }.toSeq.sorted.mkString("{", ",", "}")
    case (null, _) => "null"
    case (s: String, StringType) => "\"" + s + "\""
    case (decimal, DecimalType()) => decimal.toString
    case (other, tpe) if primitiveTypes contains tpe => other.toString
  }
}
