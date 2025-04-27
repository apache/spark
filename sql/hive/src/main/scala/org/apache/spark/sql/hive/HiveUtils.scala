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
import java.net.URL
import java.util.Locale

import scala.collection.mutable.HashMap
import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.util.VersionInfo
import org.apache.hive.common.util.HiveVersionInfo

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.classic.SQLContext
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.hive.client._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils


private[spark] object HiveUtils extends Logging {
  private val PATTERN_FOR_KEY_EQ_VAL = "(.+)=(.+)".r

  /** The version of hive used internally by Spark SQL. */
  val builtinHiveVersion: String = HiveVersionInfo.getVersion

  val BUILTIN_HIVE_VERSION = buildStaticConf("spark.sql.hive.version")
    .doc("The compiled, a.k.a, builtin Hive version of the Spark distribution bundled with." +
        " Note that, this a read-only conf and only used to report the built-in hive version." +
        " If you want a different metastore client for Spark to call, please refer to" +
        " spark.sql.hive.metastore.version.")
    .version("1.1.1")
    .stringConf
    .checkValue(_ == builtinHiveVersion,
      "The builtin Hive version is read-only, please use spark.sql.hive.metastore.version")
    .createWithDefault(builtinHiveVersion)

  private def isCompatibleHiveVersion(hiveVersionStr: String): Boolean = {
    Try { IsolatedClientLoader.hiveVersion(hiveVersionStr) }.isSuccess
  }

  val HIVE_METASTORE_VERSION = buildStaticConf("spark.sql.hive.metastore.version")
    .doc("Version of the Hive metastore. Available options are " +
      "<code>2.0.0</code> through <code>2.3.10</code>, " +
      "<code>3.0.0</code> through <code>3.1.3</code> and " +
      "<code>4.0.0</code> through <code>4.0.1</code>.")
    .version("1.4.0")
    .stringConf
    .checkValue(isCompatibleHiveVersion, "Unsupported Hive Metastore version")
    .createWithDefault(builtinHiveVersion)

  val HIVE_METASTORE_JARS = buildStaticConf("spark.sql.hive.metastore.jars")
    .doc(s"""
      | Location of the jars that should be used to instantiate the HiveMetastoreClient.
      | This property can be one of four options:
      | 1. "builtin"
      |   Use Hive ${builtinHiveVersion}, which is bundled with the Spark assembly when
      |   <code>-Phive</code> is enabled. When this option is chosen,
      |   <code>spark.sql.hive.metastore.version</code> must be either
      |   <code>${builtinHiveVersion}</code> or not defined.
      | 2. "maven"
      |   Use Hive jars of specified version downloaded from Maven repositories.
      | 3. "path"
      |   Use Hive jars configured by `spark.sql.hive.metastore.jars.path`
      |   in comma separated format. Support both local or remote paths.The provided jars
      |   should be the same version as `${HIVE_METASTORE_VERSION.key}`.
      | 4. A classpath in the standard format for both Hive and Hadoop. The provided jars
      |   should be the same version as `${HIVE_METASTORE_VERSION.key}`.
      """.stripMargin)
    .version("1.4.0")
    .stringConf
    .createWithDefault("builtin")

  val HIVE_METASTORE_JARS_PATH = buildStaticConf("spark.sql.hive.metastore.jars.path")
    .doc(s"""
      | Comma-separated paths of the jars that used to instantiate the HiveMetastoreClient.
      | This configuration is useful only when `${HIVE_METASTORE_JARS.key}` is set as `path`.
      | The paths can be any of the following format:
      | 1. file://path/to/jar/foo.jar
      | 2. hdfs://nameservice/path/to/jar/foo.jar
      | 3. /path/to/jar/ (path without URI scheme follow conf `fs.defaultFS`'s URI schema)
      | 4. [http/https/ftp]://path/to/jar/foo.jar
      | Note that 1, 2, and 3 support wildcard. For example:
      | 1. file://path/to/jar/*,file://path2/to/jar/*/*.jar
      | 2. hdfs://nameservice/path/to/jar/*,hdfs://nameservice2/path/to/jar/*/*.jar
      """.stripMargin)
    .version("3.1.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val CONVERT_METASTORE_PARQUET = buildConf("spark.sql.hive.convertMetastoreParquet")
    .doc("When set to true, the built-in Parquet reader and writer are used to process " +
      "parquet tables created by using the HiveQL syntax, instead of Hive serde.")
    .version("1.1.1")
    .booleanConf
    .createWithDefault(true)

  val CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING =
    buildConf("spark.sql.hive.convertMetastoreParquet.mergeSchema")
      .doc("When true, also tries to merge possibly different but compatible Parquet schemas in " +
        "different Parquet data files. This configuration is only effective " +
        "when \"spark.sql.hive.convertMetastoreParquet\" is true.")
      .version("1.3.1")
      .booleanConf
      .createWithDefault(false)

  val CONVERT_METASTORE_ORC = buildConf("spark.sql.hive.convertMetastoreOrc")
    .doc("When set to true, the built-in ORC reader and writer are used to process " +
      "ORC tables created by using the HiveQL syntax, instead of Hive serde.")
    .version("2.0.0")
    .booleanConf
    .createWithDefault(true)

  val CONVERT_INSERTING_PARTITIONED_TABLE =
    buildConf("spark.sql.hive.convertInsertingPartitionedTable")
      .doc("When set to true, and `spark.sql.hive.convertMetastoreParquet` or " +
        "`spark.sql.hive.convertMetastoreOrc` is true, the built-in ORC/Parquet writer is used" +
        "to process inserting into partitioned ORC/Parquet tables created by using the HiveSQL " +
        "syntax.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val CONVERT_INSERTING_UNPARTITIONED_TABLE =
    buildConf("spark.sql.hive.convertInsertingUnpartitionedTable")
      .doc("When set to true, and `spark.sql.hive.convertMetastoreParquet` or " +
        "`spark.sql.hive.convertMetastoreOrc` is true, the built-in ORC/Parquet writer is used" +
        "to process inserting into unpartitioned ORC/Parquet tables created by using the HiveSQL " +
        "syntax.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  val CONVERT_METASTORE_CTAS = buildConf("spark.sql.hive.convertMetastoreCtas")
    .doc("When set to true,  Spark will try to use built-in data source writer " +
      "instead of Hive serde in CTAS. This flag is effective only if " +
      "`spark.sql.hive.convertMetastoreParquet` or `spark.sql.hive.convertMetastoreOrc` is " +
      "enabled respectively for Parquet and ORC formats")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(true)

  val CONVERT_METASTORE_INSERT_DIR = buildConf("spark.sql.hive.convertMetastoreInsertDir")
    .doc("When set to true,  Spark will try to use built-in data source writer " +
      "instead of Hive serde in INSERT OVERWRITE DIRECTORY. This flag is effective only if " +
      "`spark.sql.hive.convertMetastoreParquet` or `spark.sql.hive.convertMetastoreOrc` is " +
      "enabled respectively for Parquet and ORC formats")
    .version("3.3.0")
    .booleanConf
    .createWithDefault(true)

  val HIVE_METASTORE_SHARED_PREFIXES = buildStaticConf("spark.sql.hive.metastore.sharedPrefixes")
    .doc("A comma separated list of class prefixes that should be loaded using the classloader " +
      "that is shared between Spark SQL and a specific version of Hive. An example of classes " +
      "that should be shared is JDBC drivers that are needed to talk to the metastore. Other " +
      "classes that need to be shared are those that interact with classes that are already " +
      "shared. For example, custom appenders that are used by log4j.")
    .version("1.4.0")
    .stringConf
    .toSequence
    .createWithDefault(jdbcPrefixes)

  private def jdbcPrefixes = Seq(
    "com.mysql.jdbc", "com.mysql.cj", "org.postgresql", "com.microsoft.sqlserver", "oracle.jdbc")

  val HIVE_METASTORE_BARRIER_PREFIXES = buildStaticConf("spark.sql.hive.metastore.barrierPrefixes")
    .doc("A comma separated list of class prefixes that should explicitly be reloaded for each " +
      "version of Hive that Spark SQL is communicating with. For example, Hive UDFs that are " +
      "declared in a prefix that typically would be shared (i.e. <code>org.apache.spark.*</code>).")
    .version("1.4.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val HIVE_THRIFT_SERVER_ASYNC = buildConf("spark.sql.hive.thriftServer.async")
    .doc("When set to true, Hive Thrift server executes SQL queries in an asynchronous way.")
    .version("1.5.0")
    .booleanConf
    .createWithDefault(true)

  val USE_DELEGATE_FOR_SYMLINK_TEXT_INPUT_FORMAT =
    buildConf("spark.sql.hive.useDelegateForSymlinkTextInputFormat")
      .internal()
      .doc("When true, SymlinkTextInputFormat is replaced with a similar delegate class during " +
        "table scan in order to fix the issue of empty splits")
      .version("3.4.0")
      .booleanConf
      .createWithDefault(true)

  /**
   * The version of the hive client that will be used to communicate with the metastore.  Note that
   * this does not necessarily need to be the same version of Hive that is used internally by
   * Spark SQL for execution.
   */
  private def hiveMetastoreVersion(conf: SQLConf): String = {
    conf.getConf(HIVE_METASTORE_VERSION)
  }

  /**
   * The location of the jars that should be used to instantiate the HiveMetastoreClient.  This
   * property can be one of three options:
   *  - a classpath in the standard format for both hive and hadoop.
   *  - path - attempt to discover the jars with paths configured by `HIVE_METASTORE_JARS_PATH`.
   *  - builtin - attempt to discover the jars that were used to load Spark SQL and use those. This
   *              option is only valid when using the execution version of Hive.
   *  - maven - download the correct version of hive on demand from maven.
   */
  private def hiveMetastoreJars(conf: SQLConf): String = {
    conf.getConf(HIVE_METASTORE_JARS)
  }

  /**
   * Hive jars paths, only work when `HIVE_METASTORE_JARS` is `path`.
   */
  private def hiveMetastoreJarsPath(conf: SQLConf): Seq[String] = {
    conf.getConf(HIVE_METASTORE_JARS_PATH)
  }

  /**
   * A comma separated list of class prefixes that should be loaded using the classloader that
   * is shared between Spark SQL and a specific version of Hive. An example of classes that should
   * be shared is JDBC drivers that are needed to talk to the metastore. Other classes that need
   * to be shared are those that interact with classes that are already shared.  For example,
   * custom appenders that are used by log4j.
   */
  private def hiveMetastoreSharedPrefixes(conf: SQLConf): Seq[String] = {
    conf.getConf(HIVE_METASTORE_SHARED_PREFIXES).filterNot(_ == "")
  }

  /**
   * A comma separated list of class prefixes that should explicitly be reloaded for each version
   * of Hive that Spark SQL is communicating with.  For example, Hive UDFs that are declared in a
   * prefix that typically would be shared (i.e. org.apache.spark.*)
   */
  private def hiveMetastoreBarrierPrefixes(conf: SQLConf): Seq[String] = {
    conf.getConf(HIVE_METASTORE_BARRIER_PREFIXES).filterNot(_ == "")
  }

  /**
   * Check current Thread's SessionState type
   * @return true when SessionState.get returns an instance of CliSessionState,
   *         false when it gets non-CliSessionState instance or null
   */
  def isCliSessionState(): Boolean = {
    val state = SessionState.get
    var temp: Class[_] = if (state != null) state.getClass else null
    var found = false
    while (temp != null && !found) {
      found = temp.getName == "org.apache.hadoop.hive.cli.CliSessionState"
      temp = temp.getSuperclass
    }
    found
  }

  /**
   * Create a [[HiveClient]] used for execution.
   *
   * Currently this must always be the Hive built-in version that packaged
   * with Spark SQL. This copy of the client is used for execution related tasks like
   * registering temporary functions or ensuring that the ThreadLocal SessionState is
   * correctly populated.  This copy of Hive is *not* used for storing persistent metadata,
   * and only point to a dummy metastore in a temporary directory.
   */
  protected[hive] def newClientForExecution(
      conf: SparkConf,
      hadoopConf: Configuration): HiveClientImpl = {
    logInfo(log"Initializing execution hive, version " +
      log"${MDC(LogKeys.HIVE_METASTORE_VERSION, builtinHiveVersion)}")
    val loader = new IsolatedClientLoader(
      version = IsolatedClientLoader.hiveVersion(builtinHiveVersion),
      sparkConf = conf,
      execJars = Seq.empty,
      hadoopConf = hadoopConf,
      config = newTemporaryConfiguration(useInMemoryDerby = true),
      isolationOn = false,
      baseClassLoader = Utils.getContextOrSparkClassLoader)
    loader.createClient().asInstanceOf[HiveClientImpl]
  }

  /**
   * Create a [[HiveClient]] used to retrieve metadata from the Hive MetaStore.
   *
   * The version of the Hive client that is used here must match the metastore that is configured
   * in the hive-site.xml file.
   */
  protected[hive] def newClientForMetadata(
      conf: SparkConf,
      hadoopConf: Configuration,
      configurations: Map[String, String] = Map.empty): HiveClient = {
    val sqlConf = new SQLConf
    sqlConf.setConf(SQLContext.getSQLProperties(conf))
    val hiveMetastoreVersion = HiveUtils.hiveMetastoreVersion(sqlConf)
    val hiveMetastoreJars = HiveUtils.hiveMetastoreJars(sqlConf)
    val hiveMetastoreSharedPrefixes = HiveUtils.hiveMetastoreSharedPrefixes(sqlConf)
    val hiveMetastoreBarrierPrefixes = HiveUtils.hiveMetastoreBarrierPrefixes(sqlConf)
    val metaVersion = IsolatedClientLoader.hiveVersion(hiveMetastoreVersion)

    def addLocalHiveJars(file: File): Seq[URL] = {
      if (file.getName == "*") {
        val files = file.getParentFile.listFiles()
        if (files == null) {
          logWarning(log"Hive jar path '${MDC(LogKeys.PATH, file.getPath)}' does not exist.")
          Nil
        } else {
          files.filter(_.getName.toLowerCase(Locale.ROOT).endsWith(".jar")).map(_.toURI.toURL)
            .toImmutableArraySeq
        }
      } else {
        file.toURI.toURL :: Nil
      }
    }

    def logInitWithPath(jars: Seq[URL]): Unit = {
      logInfo(log"Initializing HiveMetastoreConnection version " +
        log"${MDC(LogKeys.HIVE_METASTORE_VERSION, hiveMetastoreVersion)} using paths: " +
        log"${MDC(LogKeys.PATH, jars.mkString(", "))}")
    }

    val isolatedLoader = if (hiveMetastoreJars == "builtin") {
      if (builtinHiveVersion != hiveMetastoreVersion) {
        throw new IllegalArgumentException(
          "Builtin jars can only be used when hive execution version == hive metastore version. " +
            s"Execution: $builtinHiveVersion != Metastore: $hiveMetastoreVersion. " +
            s"Specify a valid path to the correct hive jars using ${HIVE_METASTORE_JARS.key} " +
            s"or change ${HIVE_METASTORE_VERSION.key} to $builtinHiveVersion.")
      }

      logInfo(
        log"Initializing HiveMetastoreConnection version " +
          log"${MDC(LogKeys.HIVE_METASTORE_VERSION, hiveMetastoreVersion)} using Spark classes.")
      new IsolatedClientLoader(
        version = metaVersion,
        sparkConf = conf,
        hadoopConf = hadoopConf,
        config = configurations,
        isolationOn = false,
        sessionStateIsolationOverride = Some(!isCliSessionState()),
        barrierPrefixes = hiveMetastoreBarrierPrefixes,
        sharedPrefixes = hiveMetastoreSharedPrefixes)
    } else if (hiveMetastoreJars == "maven") {
      // TODO: Support for loading the jars from an already downloaded location.
      logInfo(
        log"Initializing HiveMetastoreConnection version " +
          log"${MDC(LogKeys.HIVE_METASTORE_VERSION, hiveMetastoreVersion)} using maven.")
      IsolatedClientLoader.forVersion(
        hiveMetastoreVersion = hiveMetastoreVersion,
        hadoopVersion = VersionInfo.getVersion,
        sparkConf = conf,
        hadoopConf = hadoopConf,
        config = configurations,
        barrierPrefixes = hiveMetastoreBarrierPrefixes,
        sharedPrefixes = hiveMetastoreSharedPrefixes)
    } else if (hiveMetastoreJars == "path") {
      // Convert to files and expand any directories.
      val jars =
        HiveUtils.hiveMetastoreJarsPath(sqlConf)
          .flatMap {
            case path if path.contains("\\") && Utils.isWindows =>
              addLocalHiveJars(new File(path))
            case path =>
              DataSource.checkAndGlobPathIfNecessary(
                pathStrings = Seq(path),
                hadoopConf = hadoopConf,
                checkEmptyGlobPath = true,
                checkFilesExist = false,
                enableGlobbing = true
              ).map(_.toUri.toURL)
          }

      logInitWithPath(jars)
      new IsolatedClientLoader(
        version = metaVersion,
        sparkConf = conf,
        hadoopConf = hadoopConf,
        execJars = jars,
        config = configurations,
        isolationOn = true,
        barrierPrefixes = hiveMetastoreBarrierPrefixes,
        sharedPrefixes = hiveMetastoreSharedPrefixes)
    } else {
      // Convert to files and expand any directories.
      val jars =
        hiveMetastoreJars
          .split(File.pathSeparator)
          .flatMap { path =>
            addLocalHiveJars(new File(path))
          }

      logInitWithPath(jars.toSeq)
      new IsolatedClientLoader(
        version = metaVersion,
        sparkConf = conf,
        hadoopConf = hadoopConf,
        execJars = jars.toImmutableArraySeq,
        config = configurations,
        isolationOn = true,
        barrierPrefixes = hiveMetastoreBarrierPrefixes,
        sharedPrefixes = hiveMetastoreSharedPrefixes)
    }
    isolatedLoader.createClient()
  }

  /** Constructs a configuration for hive, where the metastore is located in a temp directory. */
  def newTemporaryConfiguration(useInMemoryDerby: Boolean): Map[String, String] = {
    val withInMemoryMode = if (useInMemoryDerby) "memory:" else ""

    val tempDir = Utils.createTempDir()
    val localMetastore = new File(tempDir, "metastore")
    val propMap: HashMap[String, String] = HashMap()
    // We have to mask all properties in hive-site.xml that relates to metastore data source
    // as we used a local metastore here.
    HiveConf.ConfVars.values().foreach { confvar =>
      if (confvar.varname.contains("datanucleus") || confvar.varname.contains("jdo")
        || confvar.varname.contains("hive.metastore.rawstore.impl")) {
        propMap.put(confvar.varname, confvar.getDefaultExpr())
      }
    }
    propMap.put(WAREHOUSE_PATH.key, localMetastore.toURI.toString)
    propMap.put("javax.jdo.option.ConnectionURL",
      s"jdbc:derby:${withInMemoryMode};databaseName=${localMetastore.getAbsolutePath};create=true")
    propMap.put("datanucleus.rdbms.datastoreAdapterClassName",
      "org.datanucleus.store.rdbms.adapter.DerbyAdapter")

    // Disable schema verification and allow schema auto-creation in the
    // Derby database, in case the config for the metastore is set otherwise.
    // Without these settings, starting the client fails with
    // MetaException(message:Version information not found in metastore.)
    propMap.put("hive.metastore.schema.verification", "false")
    propMap.put("datanucleus.schema.autoCreateAll", "true")

    // SPARK-11783: When "hive.metastore.uris" is set, the metastore connection mode will be
    // remote (https://cwiki.apache.org/confluence/display/Hive/AdminManual+MetastoreAdmin
    // mentions that "If hive.metastore.uris is empty local mode is assumed, remote otherwise").
    // Remote means that the metastore server is running in its own process.
    // When the mode is remote, configurations like "javax.jdo.option.ConnectionURL" will not be
    // used (because they are used by remote metastore server that talks to the database).
    // Because execution Hive should always connects to an embedded derby metastore.
    // We have to remove the value of hive.metastore.uris. So, the execution Hive client connects
    // to the actual embedded derby metastore instead of the remote metastore.
    // You can search hive.metastore.uris in the code of HiveConf (in Hive's repo).
    // Then, you will find that the local metastore mode is only set to true when
    // hive.metastore.uris is not set.
    propMap.put("hive.metastore.uris", "")

    // The execution client will generate garbage events, therefore the listeners that are generated
    // for the execution clients are useless. In order to not output garbage, we don't generate
    // these listeners.
    propMap.put(ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname, "")
    propMap.put(ConfVars.METASTORE_EVENT_LISTENERS.varname, "")
    propMap.put(ConfVars.METASTORE_END_FUNCTION_LISTENERS.varname, "")

    // SPARK-21451: Spark will gather all `spark.hadoop.*` properties from a `SparkConf` to a
    // Hadoop Configuration internally, as long as it happens after SparkContext initialized.
    // Some instances such as `CliSessionState` used in `SparkSQLCliDriver` may also rely on these
    // Configuration. But it happens before SparkContext initialized, we need to take them from
    // system properties in the form of regular hadoop configurations.
    SparkHadoopUtil.get.appendSparkHadoopConfigs(sys.props.toMap, propMap)
    SparkHadoopUtil.get.appendSparkHiveConfigs(sys.props.toMap, propMap)

    propMap.toMap
  }

  /**
   * Infers the schema for Hive serde tables and returns the CatalogTable with the inferred schema.
   * When the tables are data source tables or the schema already exists, returns the original
   * CatalogTable.
   */
  def inferSchema(table: CatalogTable): CatalogTable = {
    if (DDLUtils.isDatasourceTable(table) || table.dataSchema.nonEmpty) {
      table
    } else {
      val hiveTable = HiveClientImpl.toHiveTable(table)
      // Note: Hive separates partition columns and the schema, but for us the
      // partition columns are part of the schema
      val partCols = hiveTable.getPartCols.asScala.map(HiveClientImpl.fromHiveColumn)
      val dataCols = hiveTable.getCols.asScala.map(HiveClientImpl.fromHiveColumn)
      table.copy(schema = StructType((dataCols ++ partCols).toArray))
    }
  }

  /**
   * Extract the partition values from a partition name, e.g., if a partition name is
   * "region=US/dt=2023-02-18", then we will return an array of values ("US", "2023-02-18").
   */
  def partitionNameToValues(name: String): Array[String] = {
    name.split(Path.SEPARATOR).map {
      case PATTERN_FOR_KEY_EQ_VAL(_, v) => FileUtils.unescapePathName(v)
    }
  }
}
