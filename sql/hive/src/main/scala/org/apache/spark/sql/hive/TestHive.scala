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

import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.io.avro.{AvroContainerInputFormat, AvroContainerOutputFormat}
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.serde2.RegexSerDe
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.hadoop.hive.serde2.avro.AvroSerDe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical.{CacheCommand, LogicalPlan, NativeCommand}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.hive._

/* Implicit conversions */
import scala.collection.JavaConversions._

object TestHive
  extends TestHiveContext(new SparkContext("local", "TestSQLContext", new SparkConf()))

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
class TestHiveContext(sc: SparkContext) extends HiveContext(sc) {
  self =>

  // By clearing the port we force Spark to pick a new one.  This allows us to rerun tests
  // without restarting the JVM.
  System.clearProperty("spark.hostPort")

  lazy val warehousePath = getTempFilePath("sparkHiveWarehouse").getCanonicalPath
  lazy val metastorePath = getTempFilePath("sparkHiveMetastore").getCanonicalPath

  /** Sets up the system initially or after a RESET command */
  protected def configure() {
    setConf("javax.jdo.option.ConnectionURL",
      s"jdbc:derby:;databaseName=$metastorePath;create=true")
    setConf("hive.metastore.warehouse.dir", warehousePath)
  }

  val testTempDir = File.createTempFile("testTempFiles", "spark.hive.tmp")
  testTempDir.delete()
  testTempDir.mkdir()

  // For some hive test case which contain ${system:test.tmp.dir}
  System.setProperty("test.tmp.dir", testTempDir.getCanonicalPath)

  configure() // Must be called before initializing the catalog below.

  /** The location of the compiled hive distribution */
  lazy val hiveHome = envVarToFile("HIVE_HOME")
  /** The location of the hive source code. */
  lazy val hiveDevHome = envVarToFile("HIVE_DEV_HOME")

  // Override so we can intercept relative paths and rewrite them to point at hive.
  override def runSqlHive(sql: String): Seq[String] = super.runSqlHive(rewritePaths(sql))

  override def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution { val logical = plan }

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
      cmd.replaceAll("\\.\\.", testDataLocation)
    } else {
      cmd
    }

  val hiveFilesTemp = File.createTempFile("catalystHiveFiles", "")
  hiveFilesTemp.delete()
  hiveFilesTemp.mkdir()
  hiveFilesTemp.deleteOnExit()


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

  protected[hive] class HiveQLQueryExecution(hql: String) extends this.QueryExecution {
    lazy val logical = HiveQl.parseSql(hql)
    def hiveExec() = runSqlHive(hql)
    override def toString = hql + "\n" + super.toString
  }

  /**
   * Override QueryExecution with special debug workflow.
   */
  abstract class QueryExecution extends super.QueryExecution {
    override lazy val analyzed = {
      val describedTables = logical match {
        case NativeCommand(describedTable(tbl)) => tbl :: Nil
        case CacheCommand(tbl, _) => tbl :: Nil
        case _ => Nil
      }

      // Make sure any test tables referenced are loaded.
      val referencedTables =
        describedTables ++
        logical.collect { case UnresolvedRelation(databaseName, name, _) => name }
      val referencedTestTables = referencedTables.filter(testTables.contains)
      logDebug(s"Query references test tables: ${referencedTestTables.mkString(", ")}")
      referencedTestTables.foreach(loadTestTable)
      // Proceed with analysis.
      analyzer(logical)
    }
  }

  case class TestTable(name: String, commands: (()=>Unit)*)

  protected[hive] implicit class SqlCmd(sql: String) {
    def cmd = () => new HiveQLQueryExecution(sql).stringResult(): Unit
  }

  /**
   * A list of test tables and the DDL required to initialize them.  A test table is loaded on
   * demand when a query are run against it.
   */
  lazy val testTables = new mutable.HashMap[String, TestTable]()
  def registerTestTable(testTable: TestTable) = testTables += (testTable.name -> testTable)

  // The test tables that are defined in the Hive QTestUtil.
  // /itests/util/src/main/java/org/apache/hadoop/hive/ql/QTestUtil.java
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
      import org.apache.thrift.protocol.TBinaryProtocol
      import org.apache.hadoop.hive.serde2.thrift.test.Complex
      import org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer
      import org.apache.hadoop.mapred.SequenceFileInputFormat
      import org.apache.hadoop.mapred.SequenceFileOutputFormat

      val srcThrift = new Table("default", "src_thrift")
      srcThrift.setFields(Nil)
      srcThrift.setInputFormatClass(classOf[SequenceFileInputFormat[_,_]].getName)
      // In Hive, SequenceFileOutputFormat will be substituted by HiveSequenceFileOutputFormat.
      srcThrift.setOutputFormatClass(classOf[SequenceFileOutputFormat[_,_]].getName)
      srcThrift.setSerializationLib(classOf[ThriftDeserializer].getName)
      srcThrift.setSerdeParam("serialization.class", classOf[Complex].getName)
      srcThrift.setSerdeParam("serialization.format", classOf[TBinaryProtocol].getName)
      catalog.client.createTable(srcThrift)


      runSqlHive(
        s"LOAD DATA LOCAL INPATH '${getHiveFile("data/files/complex.seq")}' INTO TABLE src_thrift")
    }),
    TestTable("serdeins",
      s"""CREATE TABLE serdeins (key INT, value STRING)
         |ROW FORMAT SERDE '${classOf[LazySimpleSerDe].getCanonicalName}'
         |WITH SERDEPROPERTIES ('field.delim'='\\t')
       """.stripMargin.cmd,
      "INSERT OVERWRITE TABLE serdeins SELECT * FROM src".cmd),
    TestTable("sales",
      s"""CREATE TABLE IF NOT EXISTS sales (key STRING, value INT)
         |ROW FORMAT SERDE '${classOf[RegexSerDe].getCanonicalName}'
         |WITH SERDEPROPERTIES ("input.regex" = "([^ ]*)\t([^ ]*)")
       """.stripMargin.cmd,
      s"LOAD DATA LOCAL INPATH '${getHiveFile("data/files/sales.txt")}' INTO TABLE sales".cmd),
    TestTable("episodes",
      s"""CREATE TABLE episodes (title STRING, air_date STRING, doctor INT)
         |ROW FORMAT SERDE '${classOf[AvroSerDe].getCanonicalName}'
         |STORED AS
         |INPUTFORMAT '${classOf[AvroContainerInputFormat].getCanonicalName}'
         |OUTPUTFORMAT '${classOf[AvroContainerOutputFormat].getCanonicalName}'
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
    // THIS TABLE IS NOT THE SAME AS THE HIVE TEST TABLE episodes_partitioned AS DYNAMIC PARITIONING
    // IS NOT YET SUPPORTED
    TestTable("episodes_part",
      s"""CREATE TABLE episodes_part (title STRING, air_date STRING, doctor INT)
         |PARTITIONED BY (doctor_pt INT)
         |ROW FORMAT SERDE '${classOf[AvroSerDe].getCanonicalName}'
         |STORED AS
         |INPUTFORMAT '${classOf[AvroContainerInputFormat].getCanonicalName}'
         |OUTPUTFORMAT '${classOf[AvroContainerOutputFormat].getCanonicalName}'
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
      )
  )

  hiveQTestUtilTables.foreach(registerTestTable)

  private val loadedTables = new collection.mutable.HashSet[String]

  var cacheTables: Boolean = false
  def loadTestTable(name: String) {
    if (!(loadedTables contains name)) {
      // Marks the table as loaded first to prevent infite mutually recursive table loading.
      loadedTables += name
      logInfo(s"Loading test table $name")
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
  protected val originalUdfs: JavaSet[String] = FunctionRegistry.getFunctionNames

  /**
   * Resets the test instance by deleting any tables that have been created.
   * TODO: also clear out UDFs, views, etc.
   */
  def reset() {
    try {
      // HACK: Hive is too noisy by default.
      org.apache.log4j.LogManager.getCurrentLoggers.foreach { log =>
        log.asInstanceOf[org.apache.log4j.Logger].setLevel(org.apache.log4j.Level.WARN)
      }

      // It is important that we RESET first as broken hooks that might have been set could break
      // other sql exec here.
      runSqlHive("RESET")
      // For some reason, RESET does not reset the following variables...
      runSqlHive("set datanucleus.cache.collections=true")
      runSqlHive("set datanucleus.cache.collections.lazy=true")
      // Lots of tests fail if we do not change the partition whitelist from the default.
      runSqlHive("set hive.metastore.partition.name.whitelist.pattern=.*")

      loadedTables.clear()
      catalog.client.getAllTables("default").foreach { t =>
        logDebug(s"Deleting table $t")
        val table = catalog.client.getTable("default", t)

        catalog.client.getIndexes("default", t, 255).foreach { index =>
          catalog.client.dropIndex("default", t, index.getIndexName, true)
        }

        if (!table.isIndexTable) {
          catalog.client.dropTable("default", t)
        }
      }

      catalog.client.getAllDatabases.filterNot(_ == "default").foreach { db =>
        logDebug(s"Dropping Database: $db")
        catalog.client.dropDatabase(db, true, false, true)
      }

      catalog.unregisterAllTables()

      FunctionRegistry.getFunctionNames.filterNot(originalUdfs.contains(_)).foreach { udfName =>
        FunctionRegistry.unregisterTemporaryUDF(udfName)
      }

      configure()

      runSqlHive("USE default")

      // Just loading src makes a lot of tests pass.  This is because some tests do something like
      // drop an index on src at the beginning.  Since we just pass DDL to hive this bypasses our
      // Analyzer and thus the test table auto-loading mechanism.
      // Remove after we handle more DDL operations natively.
      loadTestTable("src")
      loadTestTable("srcpart")
    } catch {
      case e: Exception =>
        logError(s"FATAL ERROR: Failed to reset TestDB state. $e")
        // At this point there is really no reason to continue, but the test framework traps exits.
        // So instead we just pause forever so that at least the developer can see where things
        // started to go wrong.
        Thread.sleep(100000)
    }
  }
}
