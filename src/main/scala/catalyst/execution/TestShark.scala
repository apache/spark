package catalyst
package execution

import java.io.File
import java.util.{Set => JavaSet}

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.language.implicitConversions

import org.apache.hadoop.hive.metastore.api.{SerDeInfo, StorageDescriptor}
import org.apache.hadoop.hive.metastore.MetaStoreUtils
import org.apache.hadoop.hive.ql.exec.FunctionRegistry

import analysis._
import plans.logical.LogicalPlan
import frontend.hive._
import util._

/**
 * A locally running test instance of spark.  The lifecycle for a given query is managed by the
 * inner class [[SharkQuery]].  A [[SharkQuery]] can either be instantiated directly or using the
 * implicit conversion '.q'.
 *
 * {{{
 *   scala> val query = "SELECT key FROM src".q
 *   query: testShark.SharkQuery =
 *   SELECT key FROM src
 *   == Logical Plan ==
 *   Project {key#2}
 *    MetastoreRelation src
 *
 *   == Physical Plan ==
 *   HiveTableScan {key#2}, MetastoreRelation src
 *
 *   scala> query.execute().get.collect()
 *   res0: Array[IndexedSeq[Any]] = Array(Vector(238), Vector(86), Vector(311), ...
 * }}}
 *
 * Data from [[testTables]] will be automatically loaded whenever a query is run over those tables.
 * Calling [[reset]] will delete all tables and other state in the database, leaving the database
 * in a "clean" state.
 *
 * TestShark is implemented as a singleton object because instantiating multiple copies of the hive
 * metastore seems to lead to weird non-deterministic failures.  Therefore, the execution of
 * testcases that rely on TestShark must be serialized.
 */
object TestShark extends SharkInstance {
  self =>

  lazy val master = "local"
  lazy val warehousePath = getTempFilePath("sharkWarehouse").getCanonicalPath
  lazy val metastorePath = getTempFilePath("sharkMetastore").getCanonicalPath

  override protected def createContext() =  {
    // By clearing the port we force Spark to pick a new one.  This allows us to rerun tests
    // without restarting the JVM.
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    SharkEnv.initWithSharkContext("catalyst.execution.TestShark", master)
  }

  /** The location of the compiled hive distribution */
  lazy val hiveHome = envVarToFile("HIVE_HOME")
  /** The location of the hive source code. */
  lazy val hiveDevHome = envVarToFile("HIVE_DEV_HOME")

  // Override so we can intercept relative paths and rewrite them to point at hive.
  override def runSqlHive(sql: String): Seq[String] = super.runSqlHive(rewritePaths(sql))

  /**
   * Returns the value of specified environmental variable as a [[java.io.File]] after checking
   * to ensure it exists
   */
  private def envVarToFile(envVar: String): File = {
    assert(System.getenv(envVar) != null, s"$envVar not set")
    val ret = new File(System.getenv(envVar))
    assert(ret.exists(), s"Specified $envVar '${ret.getCanonicalPath}' does not exist.")
    ret
  }

  /**
   * Replaces relative paths to the parent directory "../" with hiveDevHome since this is how the
   * hive test cases assume the system is set up.
   */
  private def rewritePaths(cmd: String): String =
    if (cmd.toUpperCase contains "LOAD DATA")
      cmd.replaceAll("\\.\\.", hiveDevHome.getCanonicalPath)
    else
      cmd

  val describedTable = "DESCRIBE (\\w+)".r

  /**
   * Override SharkQuery with special debug workflow.
   */
  abstract class SharkQuery extends super.SharkQuery {
    override lazy val analyzed = {
      val describedTables = parsed match {
        case NativeCommand(describedTable(tbl)) => tbl :: Nil
        case _ => Nil
      }

      // Make sure any test tables referenced are loaded.
      val referencedTables =
        describedTables ++
        parsed.collect { case UnresolvedRelation(name, _) => name.split("\\.").last }
      val referencedTestTables = referencedTables.filter(testTables.contains)
      logger.debug(s"Query references test tables: ${referencedTestTables.mkString(", ")}")
      referencedTestTables.foreach(loadTestTable)
      // Proceed with analysis.
      analyze(parsed)
    }

    /**
     * Runs the query after interposing operators that print the result of each intermediate step.
     */
    def debugExec() = DebugQuery(executedPlan).execute().collect()
  }

  class SharkSqlQuery(sql: String) extends SharkQuery {
    lazy val parsed = HiveQl.parseSql(sql)
    def hiveExec() = runSqlHive(sql)
    override def toString = sql + "\n" + super.toString
  }


  /* We must repeat the implicits so that we bind to the overriden versions */

  implicit class stringToTestQuery(str: String) {
    def q = new SharkSqlQuery(str)
  }

  implicit override def logicalToSharkQuery(plan: LogicalPlan) = new LogicalSharkQuery {
    val parsed = plan
  }

  case class TestTable(name: String, commands: (()=>Unit)*)

  implicit class SqlCmd(sql: String) { def cmd = () => sql.q.stringResult(): Unit}

  /**
   * A list of test tables and the DDL required to initialize them.  A test table is loaded on
   * demand when a query are run against it.
   */
  lazy val testTables = new mutable.HashMap[String, TestTable]()
  def registerTestTable(testTable: TestTable) = testTables += (testTable.name -> testTable)

  // The test tables that are defined in the Hive QTestUtil.
  // https://github.com/apache/hive/blob/trunk/itests/util/src/main/java/org/apache/hadoop/hive/ql/QTestUtil.java
  val hiveQTestUtilTables = Seq(
    TestTable("src",
      "CREATE TABLE src (key INT, value STRING)".cmd,
      s"LOAD DATA LOCAL INPATH '${hiveDevHome.getCanonicalPath}/data/files/kv1.txt' INTO TABLE src".cmd),
    TestTable("src1",
      "CREATE TABLE src1 (key INT, value STRING)".cmd,
      s"LOAD DATA LOCAL INPATH '${hiveDevHome.getCanonicalPath}/data/files/kv3.txt' INTO TABLE src1".cmd),
    TestTable("dest1",
      "CREATE TABLE IF NOT EXISTS dest1 (key INT, value STRING)".cmd),
    TestTable("dest2",
      "CREATE TABLE IF NOT EXISTS dest2 (key INT, value STRING)".cmd),
    TestTable("dest3",
      "CREATE TABLE IF NOT EXISTS dest3 (key INT, value STRING)".cmd),
    TestTable("srcpart", () => {
      runSqlHive("CREATE TABLE srcpart (key INT, value STRING) PARTITIONED BY (ds STRING, hr STRING)")
      Seq("2008-04-08", "2008-04-09").foreach { ds =>
        Seq("11", "12").foreach { hr =>
          val partSpec = Map("ds" -> ds, "hr" -> hr)
          runSqlHive(s"LOAD DATA LOCAL INPATH '${hiveDevHome.getCanonicalPath}/data/files/kv1.txt' OVERWRITE INTO TABLE srcpart PARTITION (ds='$ds',hr='$hr')")
        }
      }
    }),
    TestTable("src_thrift", () => {
      import org.apache.thrift.protocol.TBinaryProtocol
      import org.apache.hadoop.hive.serde2.thrift.test.Complex
      import org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer
      import org.apache.hadoop.mapred.SequenceFileInputFormat
      import org.apache.hadoop.mapred.SequenceFileOutputFormat

      val srcThrift = new org.apache.hadoop.hive.metastore.api.Table()
      srcThrift.setTableName("src_thrift")
      srcThrift.setDbName("default")
      srcThrift.setSd(new StorageDescriptor)
      srcThrift.getSd.setCols(Nil)
      srcThrift.getSd.setInputFormat(classOf[SequenceFileInputFormat[_,_]].getName)
      srcThrift.getSd.setOutputFormat(classOf[SequenceFileOutputFormat[_,_]].getName)
      srcThrift.getSd.setSerdeInfo(new SerDeInfo)
      srcThrift.getSd.getSerdeInfo.setSerializationLib(classOf[ThriftDeserializer].getName)
      srcThrift.getSd.getSerdeInfo.setParameters(Map(
        "serialization.class" -> classOf[Complex].getName,
        "serialization.format" -> classOf[TBinaryProtocol].getName))

      catalog.client.createTable(srcThrift)

      runSqlHive(s"LOAD DATA LOCAL INPATH '${hiveDevHome.getCanonicalPath}/data/files/complex.seq' INTO TABLE src_thrift")
    })
  )

  hiveQTestUtilTables.foreach(registerTestTable)

  private val loadedTables = new collection.mutable.HashSet[String]

  def loadTestTable(name: String) {
    if (!(loadedTables contains name)) {
      logger.info(s"Loading test table $name")
      val createCmds =
        testTables.get(name).map(_.commands).getOrElse(sys.error(s"Unknown test table $name"))
      createCmds.foreach(_())
      loadedTables += name
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
      org.apache.log4j.LogManager.getCurrentLoggers.foreach { logger =>
        logger.asInstanceOf[org.apache.log4j.Logger].setLevel(org.apache.log4j.Level.WARN)
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
        logger.debug(s"Deleting table $t")
        val table = catalog.client.getTable("default", t)

        catalog.client.listIndexes("default", t, 255).foreach { index =>
          catalog.client.dropIndex("default", t, index.getIndexName, true)
        }

        if (!MetaStoreUtils.isIndexTable(table)) {
          catalog.client.dropTable("default", t)
        }
      }

      catalog.client.getAllDatabases.filterNot(_ == "default").foreach { db =>
        logger.debug(s"Dropping Database: $db")
        catalog.client.dropDatabase(db, true, false, true)
      }

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
        logger.error(s"FATAL ERROR: Failed to reset TestDB state. $e")
        // At this point there is really no reason to continue, but the test framework traps exits.
        // So instead we just pause forever so that at least the developer can see where things
        // started to go wrong.
        Thread.sleep(100000)
    }
  }
}