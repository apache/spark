package catalyst
package shark2

import catalyst.expressions.AttributeReference
import catalyst.optimizer.Optimize
import java.io.File

import analysis._
import catalyst.plans.logical.LogicalPlan
import frontend.hive._
import planning._
import rules._
import shark.{SharkConfVars, SharkContext, SharkEnv}
import util._
import org.apache.spark.rdd.RDD

import collection.JavaConversions._
import org.apache.hadoop.hive.metastore.MetaStoreUtils

/**
 * A locally running test instance of spark.  The lifecycle for a given query is managed by the inner class
 * [[SharkQuery]].  A [[SharkQuery]] can either be instantiated directly or using the implicit conversion '.q'.
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
 * Data from [[testTables]] will be automatically loaded whenever a query is run over those tables.  Calling
 * [[reset]] will delete all tables and other state in the database, leaving the database in a "clean" state.
 *
 * TestShark is implemented as a singleton object because instantiating multiple copies of the hive metastore
 * seems to lead to weird non-deterministic failures.  Therefore, the execution of testcases that rely on TestShark
 * must be serialized.
 */
object TestShark extends Logging {
  self =>

  val WAREHOUSE_PATH = getTempFilePath("sharkWarehouse")
  val METASTORE_PATH = getTempFilePath("sharkMetastore")
  val MASTER = "local"

  /** A local shark context */
  protected val sc = {
    // By clearing the port we force Spark to pick a new one.  This allows us to rerun tests
    // without restarting the JVM.
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    SharkEnv.initWithSharkContext("shark-sql-suite-testing", MASTER)
  }

  configure()

  /** The location of the compiled hive distribution */
  lazy val hiveHome = envVarToFile("HIVE_HOME")
  /** The location of the hive source code. */
  lazy val hiveDevHome = envVarToFile("HIVE_DEV_HOME")

  /* A catalyst metadata catalog that points to the Shark/Hive Metastore. */
  val catalog = new HiveMetastoreCatalog(SharkContext.hiveconf)
  /* An analyzer that uses the Shark/Hive metastore. */
  val analyze = new Analyzer(catalog, HiveFunctionRegistry)

  /** Sets up the system initially or after a RESET command */
  protected def configure() {
    // Use hive natively for queries that won't be executed by catalyst. This is because
    // shark has dependencies on a custom version of hive that we are trying to avoid
    // in catalyst.
    SharkConfVars.setVar(SharkContext.hiveconf, SharkConfVars.EXEC_MODE, "hive")

    runSqlHive("set javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" + METASTORE_PATH + ";create=true")
    runSqlHive("set hive.metastore.warehouse.dir=" + WAREHOUSE_PATH)

    // HACK: Hive is too noisy by default.
    org.apache.log4j.LogManager.getCurrentLoggers.foreach(_.asInstanceOf[org.apache.log4j.Logger].setLevel(org.apache.log4j.Level.WARN))
  }

  /**
   * Runs the specified SQL query using Hive.
   */
  def runSqlHive(sql: String): Seq[String] = {
    val maxResults = 100000
    val results = sc.sql(rewritePaths(sql), 100000)
    // It is very confusing when you only get back some of the results...
    if(results.size == maxResults) sys.error("RESULTS POSSIBLY TRUNCATED")
    results
  }

  /** Returns the value of specified environmental variable as a [[java.io.File]] after checking to ensure it exists */
  private def envVarToFile(envVar: String): File = {
    assert(System.getenv(envVar) != null, s"$envVar not set")
    val ret = new File(System.getenv(envVar))
    assert(ret.exists(), s"Specified $envVar '${ret.getCanonicalPath}' does not exist.")
    ret
  }

  /**
   * Replaces relative paths to the parent directory "../" with hiveDevHome since this is how the hive test cases
   * assume the system is set up.
   */
  private def rewritePaths(cmd: String): String =
    if(cmd startsWith "LOAD")
      cmd.replaceAll("\\.\\.", hiveDevHome.getCanonicalPath)
    else
      cmd

  object TrivalPlanner extends QueryPlanner[SharkPlan] with PlanningStrategies {
    val sc = self.sc
    val strategies =
      SparkEquiInnerJoin ::
      SparkAggregates ::
      HiveTableScans ::
      DataSinks ::
      BasicOperators ::
      CartesianProduct :: Nil
  }

  object PrepareForExecution extends RuleExecutor[SharkPlan] {
    val batches =
      Batch("Prepare Expressions", Once,
        expressions.BindReferences) :: Nil
  }

  class SharkSqlQuery(sql: String) extends SharkQuery {
    lazy val parsed = HiveQl.parseSql(sql)
    def hiveExec() = runSqlHive(sql)
    override def toString = sql + "\n" + super.toString
  }

  abstract class SharkQuery {
    def parsed: LogicalPlan

    lazy val analyzed = {
      // Make sure any test tables referenced are loaded.
      val referencedTables = parsed collect { case UnresolvedRelation(name, _) => name.split("\\.").last }
      val referencedTestTables = referencedTables.filter(testTableNames.contains)
      logger.debug(s"Query references test tables: ${referencedTestTables.mkString(", ")}")
      referencedTestTables.foreach(loadTestTable)
      // Proceed with analysis.
      analyze(parsed)
    }
    lazy val optimizedPlan = Optimize(analyzed)
    // TODO: Don't just pick the first one...
    lazy val sharkPlan = TrivalPlanner(optimizedPlan).next()
    lazy val executedPlan = PrepareForExecution(sharkPlan)

    lazy val toRdd = executedPlan.execute()

    def debugExec() = DebugQuery(executedPlan).execute().collect

    /**
     * Returns the result as a hive compatible sequence of strings.  For native commands, the execution is simply
     * passed back to Hive.
     */
    def stringResult(): Seq[String] = analyzed match {
      case NativeCommand(cmd) => runSqlHive(rewritePaths(cmd))
      case ConfigurationAssignment(cmd) => runSqlHive(cmd)
      case ExplainCommand(plan) => (new SharkQuery { val parsed = plan }).toString.split("\n")
      case query =>
        val result: Seq[Seq[Any]] = toRdd.collect.toSeq
        // Reformat to match hive tab delimited output.
        val asString = result.map(_.map {
          case null => "NULL"
          case other => other
        }).map(_.mkString("\t")).toSeq

        asString
    }

    protected def stringOrError[A](f: => A): String =
      try f.toString catch { case e: Throwable => e.toString }

    override def toString: String =
      s"""== Logical Plan ==
         |${stringOrError(analyzed)}
         |== Physical Plan ==
         |${stringOrError(sharkPlan)}
      """.stripMargin.trim
  }

  implicit class stringToQuery(str: String) {
    def q = new SharkSqlQuery(str)
  }

  implicit def logicalToSharkQuery(plan: LogicalPlan) = new SharkQuery { val parsed = plan }

  protected case class TestTable(name: String, commands: (()=>Unit)*)


  protected implicit class SqlCmd(sql: String) { def cmd = () => sql.q.stringResult(): Unit}
  /**
   * A list of test tables and the DDL required to initialize them.  A test table is loaded on demand when a query
   * are run against it.
   */
  val testTables = Seq(
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
    })
  )
  protected val testTableNames = testTables.map(_.name).toSet

  private val loadedTables = new collection.mutable.HashSet[String]
  def loadTestTable(name: String) {
    if(!(loadedTables contains name)) {
      logger.info(s"Loading test table $name")
      val createCmds = testTables.find(_.name == name).map(_.commands).getOrElse(sys.error(s"Unknown test table $name"))
      createCmds.foreach(_())
      loadedTables += name
    }
  }

  /**
   * Resets the test instance by deleting any tables that have been created.
   * TODO: also clear out UDFs, views, etc.
   */
  def reset() {
    try {
      // It is important that we RESET first as broken hooks that might have been set could break other sql exec here.
      runSqlHive("RESET")
      // For some reason, RESET does not reset the following variables...
      runSqlHive("set datanucleus.cache.collections=true")
      runSqlHive("set datanucleus.cache.collections.lazy=true")

      loadedTables.clear()
      catalog.client.getAllTables("default").foreach(t => {
        logger.debug(s"Deleting table $t")
        val table = catalog.client.getTable("default", t)

        catalog.client.listIndexes("default", t, 255)
          .foreach(i => catalog.client.dropIndex("default", t, i.getIndexName, true))

        if(!MetaStoreUtils.isIndexTable(table))
          catalog.client.dropTable("default", t)
      })

      catalog.client.getAllDatabases.filterNot(_ == "default").foreach {db =>
        logger.debug(s"Dropping Database: $db")
        catalog.client.dropDatabase(db, true, false, true)
      }

      configure()

      runSqlHive("USE default")
    } catch {
      case e: Exception =>
        logger.error(s"FATAL ERROR: Failed to reset TestDB state. $e")
        // At this point there is really no reason to continue, but the test framework traps exits.  So instead we just
        // pause forever so that at least the developer can see where things started to go wrong.
        Thread.sleep(100000)
    }
  }
}