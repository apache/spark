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

package org.apache.spark.sql.hive.test

import java.io.File
import java.util.{Set => JavaSet}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.CacheManager
import org.apache.spark.sql.execution.command.CacheTableCommand
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.client.{HiveClient, HiveClientImpl}
import org.apache.spark.sql.hive.execution.HiveNativeCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{ShutdownHookManager, Utils}

// SPARK-3729: Test key required to check for initialization errors with config.
object TestHive
  extends TestHiveContext(
    new SparkContext(
      System.getProperty("spark.sql.test.master", "local[1]"),
      "TestSQLContext",
      new SparkConf()
        .set("spark.sql.test", "")
        .set("spark.sql.hive.metastore.barrierPrefixes",
          "org.apache.spark.sql.hive.execution.PairSerDe")
        // SPARK-8910
        .set("spark.ui.enabled", "false")))


/**
 * A locally running test instance of Spark's Hive execution engine.
 *
 * Data from [[testTables]] will be automatically loaded whenever a query is run over those tables.
 * Calling [[reset]] will delete all tables and other state in the database, leaving the database
 * in a "clean" state.
 *
 * TestHive is singleton object version of this class because instantiating multiple copies of the
 * hive metastore seems to lead to weird non-deterministic failures.  Therefore, the execution of
 * test cases that rely on TestHive must be serialized.
 */
class TestHiveContext private[hive](
    sc: SparkContext,
    cacheManager: CacheManager,
    listener: SQLListener,
    executionHive: HiveClientImpl,
    metadataHive: HiveClient,
    isRootContext: Boolean,
    hiveCatalog: HiveExternalCatalog,
    val warehousePath: File,
    val scratchDirPath: File,
    metastoreTemporaryConf: Map[String, String])
  extends HiveContext(
    sc,
    cacheManager,
    listener,
    executionHive,
    metadataHive,
    isRootContext,
    hiveCatalog) { self =>

  // Unfortunately, due to the complex interactions between the construction parameters
  // and the limitations in scala constructors, we need many of these constructors to
  // provide a shorthand to create a new TestHiveContext with only a SparkContext.
  // This is not a great design pattern but it's necessary here.

  private def this(
      sc: SparkContext,
      executionHive: HiveClientImpl,
      metadataHive: HiveClient,
      warehousePath: File,
      scratchDirPath: File,
      metastoreTemporaryConf: Map[String, String]) {
    this(
      sc,
      new CacheManager,
      SQLContext.createListenerAndUI(sc),
      executionHive,
      metadataHive,
      true,
      new HiveExternalCatalog(metadataHive),
      warehousePath,
      scratchDirPath,
      metastoreTemporaryConf)
  }

  private def this(
      sc: SparkContext,
      warehousePath: File,
      scratchDirPath: File,
      metastoreTemporaryConf: Map[String, String]) {
    this(
      sc,
      HiveContext.newClientForExecution(sc.conf, sc.hadoopConfiguration),
      TestHiveContext.newClientForMetadata(
        sc.conf, sc.hadoopConfiguration, warehousePath, scratchDirPath, metastoreTemporaryConf),
      warehousePath,
      scratchDirPath,
      metastoreTemporaryConf)
  }

  def this(sc: SparkContext) {
    this(
      sc,
      Utils.createTempDir(namePrefix = "warehouse"),
      TestHiveContext.makeScratchDir(),
      HiveContext.newTemporaryConfiguration(useInMemoryDerby = false))
  }

  override def newSession(): HiveContext = {
    new TestHiveContext(
      sc = sc,
      cacheManager = cacheManager,
      listener = listener,
      executionHive = executionHive.newSession(),
      metadataHive = metadataHive.newSession(),
      isRootContext = false,
      hiveCatalog = hiveCatalog,
      warehousePath = warehousePath,
      scratchDirPath = scratchDirPath,
      metastoreTemporaryConf = metastoreTemporaryConf)
  }

  // By clearing the port we force Spark to pick a new one.  This allows us to rerun tests
  // without restarting the JVM.
  System.clearProperty("spark.hostPort")
  CommandProcessorFactory.clean(hiveconf)

  hiveconf.set("hive.plan.serialization.format", "javaXML")

  // A snapshot of the entries in the starting SQLConf
  // We save this because tests can mutate this singleton object if they want
  val initialSQLConf: SQLConf = {
    val snapshot = new SQLConf
    conf.getAllConfs.foreach { case (k, v) => snapshot.setConfString(k, v) }
    snapshot
  }

  val testTempDir = Utils.createTempDir()

  // For some hive test case which contain ${system:test.tmp.dir}
  System.setProperty("test.tmp.dir", testTempDir.getCanonicalPath)

  /** The location of the compiled hive distribution */
  lazy val hiveHome = envVarToFile("HIVE_HOME")
  /** The location of the hive source code. */
  lazy val hiveDevHome = envVarToFile("HIVE_DEV_HOME")

  // Override so we can intercept relative paths and rewrite them to point at hive.
  override def runSqlHive(sql: String): Seq[String] =
    super.runSqlHive(rewritePaths(substitutor.substitute(this.hiveconf, sql)))

  override def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution(plan)

  @transient
  protected[sql] override lazy val sessionState = new HiveSessionState(this) {
    override lazy val conf: SQLConf = {
      new SQLConf {
        clear()
        override def caseSensitiveAnalysis: Boolean = getConf(SQLConf.CASE_SENSITIVE, false)
        override def clear(): Unit = {
          super.clear()
          TestHiveContext.overrideConfs.map {
            case (key, value) => setConfString(key, value)
          }
        }
      }
    }

    override lazy val functionRegistry = {
      // We use TestHiveFunctionRegistry at here to track functions that have been explicitly
      // unregistered (through TestHiveFunctionRegistry.unregisterFunction method).
      val fr = new TestHiveFunctionRegistry
      org.apache.spark.sql.catalyst.analysis.FunctionRegistry.expressions.foreach {
        case (name, (info, builder)) => fr.registerFunction(name, info, builder)
      }
      fr
    }
  }

  /**
   * Returns the value of specified environmental variable as a [[java.io.File]] after checking
   * to ensure it exists
   */
  private def envVarToFile(envVar: String): Option[File] = {
    Option(System.getenv(envVar)).map(new File(_))
  }

  /**
   * Replaces relative paths to the parent directory "../" with hiveDevHome since this is how the
   * hive test cases assume the system is set up.
   */
  private def rewritePaths(cmd: String): String =
    if (cmd.toUpperCase contains "LOAD DATA") {
      val testDataLocation =
        hiveDevHome.map(_.getCanonicalPath).getOrElse(inRepoTests.getCanonicalPath)
      cmd.replaceAll("\\.\\./\\.\\./", testDataLocation + "/")
    } else {
      cmd
    }

  val hiveFilesTemp = File.createTempFile("catalystHiveFiles", "")
  hiveFilesTemp.delete()
  hiveFilesTemp.mkdir()
  ShutdownHookManager.registerShutdownDeleteDir(hiveFilesTemp)

  val inRepoTests = if (System.getProperty("user.dir").endsWith("sql" + File.separator + "hive")) {
    new File("src" + File.separator + "test" + File.separator + "resources" + File.separator)
  } else {
    new File("sql" + File.separator + "hive" + File.separator + "src" + File.separator + "test" +
      File.separator + "resources")
  }

  def getHiveFile(path: String): File = {
    val stripped = path.replaceAll("""\.\.\/""", "").replace('/', File.separatorChar)
    hiveDevHome
      .map(new File(_, stripped))
      .filter(_.exists)
      .getOrElse(new File(inRepoTests, stripped))
  }

  val describedTable = "DESCRIBE (\\w+)".r

  /**
   * Override QueryExecution with special debug workflow.
   */
  class QueryExecution(logicalPlan: LogicalPlan)
    extends super.QueryExecution(logicalPlan) {
    def this(sql: String) = this(parseSql(sql))
    override lazy val analyzed = {
      val describedTables = logical match {
        case HiveNativeCommand(describedTable(tbl)) => tbl :: Nil
        case CacheTableCommand(tbl, _, _) => tbl :: Nil
        case _ => Nil
      }

      // Make sure any test tables referenced are loaded.
      val referencedTables =
        describedTables ++
        logical.collect { case UnresolvedRelation(tableIdent, _) => tableIdent.table }
      val referencedTestTables = referencedTables.filter(testTables.contains)
      logDebug(s"Query references test tables: ${referencedTestTables.mkString(", ")}")
      referencedTestTables.foreach(loadTestTable)
      // Proceed with analysis.
      sessionState.analyzer.execute(logical)
    }
  }

  case class TestTable(name: String, commands: (() => Unit)*)

  protected[hive] implicit class SqlCmd(sql: String) {
    def cmd: () => Unit = {
      () => new QueryExecution(sql).stringResult(): Unit
    }
  }

  /**
   * A list of test tables and the DDL required to initialize them.  A test table is loaded on
   * demand when a query are run against it.
   */
  @transient
  lazy val testTables = new mutable.HashMap[String, TestTable]()

  def registerTestTable(testTable: TestTable): Unit = {
    testTables += (testTable.name -> testTable)
  }

  // The test tables that are defined in the Hive QTestUtil.
  // /itests/util/src/main/java/org/apache/hadoop/hive/ql/QTestUtil.java
  // https://github.com/apache/hive/blob/branch-0.13/data/scripts/q_test_init.sql
  @transient
  val hiveQTestUtilTables = Seq(
    TestTable("src",
      "CREATE TABLE src (key INT, value STRING)".cmd,
      s"LOAD DATA LOCAL INPATH '${getHiveFile("data/files/kv1.txt")}' INTO TABLE src".cmd),
    TestTable("src1",
      "CREATE TABLE src1 (key INT, value STRING)".cmd,
      s"LOAD DATA LOCAL INPATH '${getHiveFile("data/files/kv3.txt")}' INTO TABLE src1".cmd),
    TestTable("srcpart", () => {
      runSqlHive(
        "CREATE TABLE srcpart (key INT, value STRING) PARTITIONED BY (ds STRING, hr STRING)")
      for (ds <- Seq("2008-04-08", "2008-04-09"); hr <- Seq("11", "12")) {
        runSqlHive(
          s"""LOAD DATA LOCAL INPATH '${getHiveFile("data/files/kv1.txt")}'
             |OVERWRITE INTO TABLE srcpart PARTITION (ds='$ds',hr='$hr')
           """.stripMargin)
      }
    }),
    TestTable("srcpart1", () => {
      runSqlHive("CREATE TABLE srcpart1 (key INT, value STRING) PARTITIONED BY (ds STRING, hr INT)")
      for (ds <- Seq("2008-04-08", "2008-04-09"); hr <- 11 to 12) {
        runSqlHive(
          s"""LOAD DATA LOCAL INPATH '${getHiveFile("data/files/kv1.txt")}'
             |OVERWRITE INTO TABLE srcpart1 PARTITION (ds='$ds',hr='$hr')
           """.stripMargin)
      }
    }),
    TestTable("src_thrift", () => {
      import org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer
      import org.apache.hadoop.mapred.{SequenceFileInputFormat, SequenceFileOutputFormat}
      import org.apache.thrift.protocol.TBinaryProtocol

      runSqlHive(
        s"""
         |CREATE TABLE src_thrift(fake INT)
         |ROW FORMAT SERDE '${classOf[ThriftDeserializer].getName}'
         |WITH SERDEPROPERTIES(
         |  'serialization.class'='org.apache.spark.sql.hive.test.Complex',
         |  'serialization.format'='${classOf[TBinaryProtocol].getName}'
         |)
         |STORED AS
         |INPUTFORMAT '${classOf[SequenceFileInputFormat[_, _]].getName}'
         |OUTPUTFORMAT '${classOf[SequenceFileOutputFormat[_, _]].getName}'
        """.stripMargin)

      runSqlHive(
        s"LOAD DATA LOCAL INPATH '${getHiveFile("data/files/complex.seq")}' INTO TABLE src_thrift")
    }),
    TestTable("serdeins",
      s"""CREATE TABLE serdeins (key INT, value STRING)
         |ROW FORMAT SERDE '${classOf[LazySimpleSerDe].getCanonicalName}'
         |WITH SERDEPROPERTIES ('field.delim'='\\t')
       """.stripMargin.cmd,
      "INSERT OVERWRITE TABLE serdeins SELECT * FROM src".cmd),
    TestTable("episodes",
      s"""CREATE TABLE episodes (title STRING, air_date STRING, doctor INT)
         |STORED AS avro
         |TBLPROPERTIES (
         |  'avro.schema.literal'='{
         |    "type": "record",
         |    "name": "episodes",
         |    "namespace": "testing.hive.avro.serde",
         |    "fields": [
         |      {
         |          "name": "title",
         |          "type": "string",
         |          "doc": "episode title"
         |      },
         |      {
         |          "name": "air_date",
         |          "type": "string",
         |          "doc": "initial date"
         |      },
         |      {
         |          "name": "doctor",
         |          "type": "int",
         |          "doc": "main actor playing the Doctor in episode"
         |      }
         |    ]
         |  }'
         |)
       """.stripMargin.cmd,
      s"LOAD DATA LOCAL INPATH '${getHiveFile("data/files/episodes.avro")}' INTO TABLE episodes".cmd
    ),
    // THIS TABLE IS NOT THE SAME AS THE HIVE TEST TABLE episodes_partitioned AS DYNAMIC
    // PARTITIONING IS NOT YET SUPPORTED
    TestTable("episodes_part",
      s"""CREATE TABLE episodes_part (title STRING, air_date STRING, doctor INT)
         |PARTITIONED BY (doctor_pt INT)
         |STORED AS avro
         |TBLPROPERTIES (
         |  'avro.schema.literal'='{
         |    "type": "record",
         |    "name": "episodes",
         |    "namespace": "testing.hive.avro.serde",
         |    "fields": [
         |      {
         |          "name": "title",
         |          "type": "string",
         |          "doc": "episode title"
         |      },
         |      {
         |          "name": "air_date",
         |          "type": "string",
         |          "doc": "initial date"
         |      },
         |      {
         |          "name": "doctor",
         |          "type": "int",
         |          "doc": "main actor playing the Doctor in episode"
         |      }
         |    ]
         |  }'
         |)
       """.stripMargin.cmd,
      // WORKAROUND: Required to pass schema to SerDe for partitioned tables.
      // TODO: Pass this automatically from the table to partitions.
      s"""
         |ALTER TABLE episodes_part SET SERDEPROPERTIES (
         |  'avro.schema.literal'='{
         |    "type": "record",
         |    "name": "episodes",
         |    "namespace": "testing.hive.avro.serde",
         |    "fields": [
         |      {
         |          "name": "title",
         |          "type": "string",
         |          "doc": "episode title"
         |      },
         |      {
         |          "name": "air_date",
         |          "type": "string",
         |          "doc": "initial date"
         |      },
         |      {
         |          "name": "doctor",
         |          "type": "int",
         |          "doc": "main actor playing the Doctor in episode"
         |      }
         |    ]
         |  }'
         |)
        """.stripMargin.cmd,
      s"""
        INSERT OVERWRITE TABLE episodes_part PARTITION (doctor_pt=1)
        SELECT title, air_date, doctor FROM episodes
      """.cmd
      ),
    TestTable("src_json",
      s"""CREATE TABLE src_json (json STRING) STORED AS TEXTFILE
       """.stripMargin.cmd,
      s"LOAD DATA LOCAL INPATH '${getHiveFile("data/files/json.txt")}' INTO TABLE src_json".cmd)
  )

  hiveQTestUtilTables.foreach(registerTestTable)

  private val loadedTables = new collection.mutable.HashSet[String]

  var cacheTables: Boolean = false
  def loadTestTable(name: String) {
    if (!(loadedTables contains name)) {
      // Marks the table as loaded first to prevent infinite mutually recursive table loading.
      loadedTables += name
      logDebug(s"Loading test table $name")
      val createCmds =
        testTables.get(name).map(_.commands).getOrElse(sys.error(s"Unknown test table $name"))
      createCmds.foreach(_())

      if (cacheTables) {
        cacheTable(name)
      }
    }
  }

  /**
   * Records the UDFs present when the server starts, so we can delete ones that are created by
   * tests.
   */
  protected val originalUDFs: JavaSet[String] = FunctionRegistry.getFunctionNames

  /**
   * Resets the test instance by deleting any tables that have been created.
   * TODO: also clear out UDFs, views, etc.
   */
  def reset() {
    try {
      // HACK: Hive is too noisy by default.
      org.apache.log4j.LogManager.getCurrentLoggers.asScala.foreach { log =>
        val logger = log.asInstanceOf[org.apache.log4j.Logger]
        if (!logger.getName.contains("org.apache.spark")) {
          logger.setLevel(org.apache.log4j.Level.WARN)
        }
      }

      cacheManager.clearCache()
      loadedTables.clear()
      sessionState.catalog.clearTempTables()
      sessionState.catalog.invalidateCache()
      metadataHive.reset()

      FunctionRegistry.getFunctionNames.asScala.filterNot(originalUDFs.contains(_)).
        foreach { udfName => FunctionRegistry.unregisterTemporaryUDF(udfName) }

      // Some tests corrupt this value on purpose, which breaks the RESET call below.
      hiveconf.set("fs.default.name", new File(".").toURI.toString)
      // It is important that we RESET first as broken hooks that might have been set could break
      // other sql exec here.
      executionHive.runSqlHive("RESET")
      metadataHive.runSqlHive("RESET")
      // For some reason, RESET does not reset the following variables...
      // https://issues.apache.org/jira/browse/HIVE-9004
      runSqlHive("set hive.table.parameters.default=")
      runSqlHive("set datanucleus.cache.collections=true")
      runSqlHive("set datanucleus.cache.collections.lazy=true")
      // Lots of tests fail if we do not change the partition whitelist from the default.
      runSqlHive("set hive.metastore.partition.name.whitelist.pattern=.*")

      // In case a test changed any of these values, restore all the original ones here.
      TestHiveContext.hiveClientConfigurations(
        hiveconf, warehousePath, scratchDirPath, metastoreTemporaryConf)
          .foreach { case (k, v) => metadataHive.runSqlHive(s"SET $k=$v") }
      defaultOverrides()

      sessionState.catalog.setCurrentDatabase("default")
    } catch {
      case e: Exception =>
        logError("FATAL ERROR: Failed to reset TestDB state.", e)
    }
  }

}

private[hive] class TestHiveFunctionRegistry extends SimpleFunctionRegistry {

  private val removedFunctions =
    collection.mutable.ArrayBuffer.empty[(String, (ExpressionInfo, FunctionBuilder))]

  def unregisterFunction(name: String): Unit = {
    functionBuilders.remove(name).foreach(f => removedFunctions += name -> f)
  }

  def restore(): Unit = {
    removedFunctions.foreach {
      case (name, (info, builder)) => registerFunction(name, info, builder)
    }
  }
}

private[hive] object TestHiveContext {

  /**
   * A map used to store all confs that need to be overridden in sql/hive unit tests.
   */
  val overrideConfs: Map[String, String] =
    Map(
      // Fewer shuffle partitions to speed up testing.
      SQLConf.SHUFFLE_PARTITIONS.key -> "5"
    )

  /**
   * Create a [[HiveClient]] used to retrieve metadata from the Hive MetaStore.
   */
  private def newClientForMetadata(
      conf: SparkConf,
      hadoopConf: Configuration,
      warehousePath: File,
      scratchDirPath: File,
      metastoreTemporaryConf: Map[String, String]): HiveClient = {
    val hiveConf = new HiveConf(hadoopConf, classOf[HiveConf])
    HiveContext.newClientForMetadata(
      conf,
      hiveConf,
      hadoopConf,
      hiveClientConfigurations(hiveConf, warehousePath, scratchDirPath, metastoreTemporaryConf))
  }

  /**
   * Configurations needed to create a [[HiveClient]].
   */
  private def hiveClientConfigurations(
      hiveconf: HiveConf,
      warehousePath: File,
      scratchDirPath: File,
      metastoreTemporaryConf: Map[String, String]): Map[String, String] = {
    HiveContext.hiveClientConfigurations(hiveconf) ++ metastoreTemporaryConf ++ Map(
      ConfVars.METASTOREWAREHOUSE.varname -> warehousePath.toURI.toString,
      ConfVars.METASTORE_INTEGER_JDO_PUSHDOWN.varname -> "true",
      ConfVars.SCRATCHDIR.varname -> scratchDirPath.toURI.toString,
      ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY.varname -> "1")
  }

  private def makeScratchDir(): File = {
    val scratchDir = Utils.createTempDir(namePrefix = "scratch")
    scratchDir.delete()
    scratchDir
  }

}
