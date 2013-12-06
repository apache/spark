package catalyst
package shark2

import catalyst.expressions.AttributeReference
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
object TestShark {
  self =>

  val WAREHOUSE_PATH = getTempFilePath("sharkWarehouse")
  val METASTORE_PATH = getTempFilePath("sharkMetastore")
  val MASTER = "local"

  protected val sc = {
    // By clearing the port we force Spark to pick a new one.  This allows us to rerun tests
    // without restarting the JVM.
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    SharkEnv.initWithSharkContext("shark-sql-suite-testing", MASTER)
  }

  configure()

  def configure() {
    // Use hive natively for queries that won't be executed by catalyst. This is because
    // shark has dependencies on a custom version of hive that we are trying to avoid
    // in catalyst.
    SharkConfVars.setVar(SharkContext.hiveconf, SharkConfVars.EXEC_MODE, "hive")

    runSqlHive("set javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" + METASTORE_PATH + ";create=true")
    runSqlHive("set hive.metastore.warehouse.dir=" + WAREHOUSE_PATH)
  }

  /**
   * Runs the specified SQL query using Hive.
   */
  def runSqlHive(sql: String) = sc.sql(rewritePaths(sql))

  /** Returns the value of specified environmental variable as a [[java.io.File]] after checking to ensure it exists */
  private def envVarToFile(envVar: String): File = {
    assert(System.getenv(envVar) != null, s"$envVar not set")
    val ret = new File(System.getenv(envVar))
    assert(ret.exists(), s"Specified $envVar '${ret.getCanonicalPath}' does not exist.")
    ret
  }

  private def rewritePaths(cmd: String): String =
    if(cmd startsWith "LOAD")
      cmd.replaceAll("\\.\\.", hiveDevHome.getCanonicalPath)
    else
      cmd

  /** The location of the compiled hive distribution */
  lazy val hiveHome = envVarToFile("HIVE_HOME")
  /** The location of the hive source code. */
  lazy val hiveDevHome = envVarToFile("HIVE_DEV_HOME")

  def loadKv1 {
    //sc.runSql("DROP TABLE IF EXISTS test")
    runSqlHive("CREATE TABLE test (key INT, val STRING)")
    // USE ENV VARS
    runSqlHive("""LOAD DATA LOCAL INPATH '/Users/marmbrus/workspace/hive/data/files/kv1.txt' INTO TABLE test""")
  }

  val catalog = new HiveMetastoreCatalog(SharkContext.hiveconf)
  val analyze = new Analyzer(catalog)

  object TrivalPlanner extends QueryPlanner[SharkPlan] with PlanningStrategies {
    val sc = self.sc
    val strategies =
      SparkAggregates ::
      HiveTableScans ::
      DataSinks ::
      BasicOperators :: Nil
  }

  object PrepareForExecution extends RuleExecutor[SharkPlan] {
    val batches =
      Batch("Prepare Expressions", Once,
        expressions.BindReferences) :: Nil
  }

  class SharkSqlQuery(sql: String) extends SharkQuery {
    lazy val parsed = Hive.parseSql(sql)
    override def toString = sql + "\n" + super.toString
  }

  abstract class SharkQuery {
    def parsed: LogicalPlan

    lazy val analyzed = {
      // Make sure any test tables referenced are loaded.
      val referencedTables = parsed collect { case UnresolvedRelation(name, _) => name }
      val referencedTestTables = referencedTables.filter(testTableNames.contains)
      println(s"Query references test tables: ${referencedTestTables.mkString(", ")}")
      referencedTestTables.foreach(loadTestTable)
      // Proceed with analysis.
      analyze(parsed)
    }
    // TODO: Don't just pick the first one...
    lazy val sharkPlan = TrivalPlanner(analyzed).next()
    lazy val executedPlan = PrepareForExecution(sharkPlan)

    lazy val toRdd = executedPlan.execute()

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

  protected case class TestTable(name: String, commands: String*)

  /**
   * A list of test tables and the DDL required to initialize them.  A test table is loaded on demand when a query
   * are run against it.
   */
  val testTables = Seq(
    TestTable("src",
      "CREATE TABLE src (key INT, value STRING)",
      "LOAD DATA LOCAL INPATH '/Users/marmbrus/workspace/hive/data/files/kv1.txt' INTO TABLE src")
  )
  protected val testTableNames = testTables.map(_.name).toSet

  private val loadedTables = new collection.mutable.HashSet[String]
  def loadTestTable(name: String) {
    if(!(loadedTables contains name)) {
      println(s"Loading test table $name")
      val createCmds = testTables.find(_.name == name).map(_.commands).getOrElse(sys.error(s"Unknown test table $name"))
      createCmds.foreach(runSqlHive)
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
        println(s"Deleting table $t")
        val table = catalog.client.getTable("default", t)

        catalog.client.listIndexes("default", t, 255)
          .foreach(i => catalog.client.dropIndex("default", t, i.getIndexName, true))

        if(!MetaStoreUtils.isIndexTable(table))
          catalog.client.dropTable("default", t)
      })

      catalog.client.getAllDatabases.filterNot(_ == "default").foreach {db =>
        println(s"Dropping Database: $db")
        catalog.client.dropDatabase(db, true, false, true)
      }

      configure()

      runSqlHive("USE default")
    } catch {
      case e: Exception =>
        println(s"FATAL ERROR: Failed to reset TestDB state. $e")
        Thread.sleep(100000)
    }
  }
}