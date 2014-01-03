package catalyst
package execution

import java.io.File

import shark.{SharkConfVars, SharkContext, SharkEnv}

import analysis.{SimpleAnalyzer, Analyzer}
import frontend.hive._
import optimizer.Optimize
import planning.QueryPlanner
import plans.logical.LogicalPlan
import rules.RuleExecutor

/**
 * Starts up an instance of shark where metadata is stored locally. An in-process metadata data is
 * created with data stored in ./metadata.  Warehouse data is stored in in ./warehouse.
 */
class LocalSharkInstance(val master: String) extends SharkInstance {
  override def warehousePath = new File("warehouse").getCanonicalPath
  override def metastorePath = new File("metastore").getCanonicalPath
}

/**
 * An instance of the shark execution engine. This class is responsible for taking queries
 * expressed either in SQl or as raw catalyst logical plans and optimizing them for execution
 * using Spark.  Additionally this class maintains the connection with the hive metadata store.
 */
abstract class SharkInstance extends Logging {
  self =>

  /** The URL of the shark master. */
  def master: String
  /** The path to the hive warehouse. */
  def warehousePath: String
  /** The path to the local metastore. */
  def metastorePath: String

  /** The SharkContext */
  lazy val sc = createContext()

  protected def createContext() = {
    SharkEnv.initWithSharkContext("catalyst.execution", master)
  }

  /** Sets up the system initially or after a RESET command */
  protected def configure() {
    // Use hive natively for queries that won't be executed by catalyst. This is because
    // shark has dependencies on a custom version of hive that we are trying to avoid in catalyst.
    SharkConfVars.setVar(SharkContext.hiveconf, SharkConfVars.EXEC_MODE, "hive")

    // TODO: refactor this so we can work with other databases.
    runSqlHive("set javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" + metastorePath + ";create=true")
    runSqlHive("set hive.metastore.warehouse.dir=" + warehousePath)
  }

  configure() // Must be called before initializing the catalog below.

  /* A catalyst metadata catalog that points to the Shark/Hive Metastore. */
  val catalog = new HiveMetastoreCatalog(SharkContext.hiveconf)

  /* An analyzer that uses the Shark/Hive metastore. */
  val analyze = new Analyzer(catalog, HiveFunctionRegistry, caseSensitive = false)

  /**
   * Runs the specified SQL query using Hive.
   */
  def runSqlHive(sql: String): Seq[String] = {
    val maxResults = 100000
    val results = sc.sql(sql, 100000)
    // It is very confusing when you only get back some of the results...
    if (results.size == maxResults) sys.error("RESULTS POSSIBLY TRUNCATED")
    results
  }

  object TrivalPlanner extends QueryPlanner[SharkPlan] with PlanningStrategies {
    val sc = self.sc
    val strategies =
      SparkEquiInnerJoin ::
      SparkAggregates ::
      HiveTableScans ::
      DataSinks ::
      BasicOperators ::
      CartesianProduct ::
      BroadcastNestedLoopJoin :: Nil
  }

  object PrepareForExecution extends RuleExecutor[SharkPlan] {
    val batches = Batch("Prepare Expressions", Once, expressions.BindReferences) :: Nil
  }

  class SharkSqlQuery(sql: String) extends SharkQuery {
    lazy val parsed = HiveQl.parseSql(sql)
    def hiveExec() = runSqlHive(sql)
    override def toString = sql + "\n" + super.toString
  }

  /**
   * The primary workflow for executing queries using Shark.  Designed to allow easy access to the
   * intermediate phases of query execution.
   */
  abstract class SharkQuery {
    def parsed: LogicalPlan

    lazy val analyzed = analyze(parsed)
    lazy val optimizedPlan = Optimize(catalog.CreateTables(analyzed))
    // TODO: Don't just pick the first one...
    lazy val sharkPlan = TrivalPlanner(optimizedPlan).next()
    lazy val executedPlan: SharkPlan = PrepareForExecution(sharkPlan)

    lazy val toRdd = executedPlan.execute()

    def toHiveString(a: Any): String = a match {
      case seq: Seq[_] => seq.map(toHiveString).map(s => "\"" + s + "\"").mkString("[", ",", "]")
      case "null" => "NULL"
      case null => "NULL"
      case other => other.toString
    }

    /**
     * Returns the result as a hive compatible sequence of strings.  For native commands, the
     * execution is simply passed back to Hive.
     */
    def stringResult(): Seq[String] = analyzed match {
      case NativeCommand(cmd) => runSqlHive(cmd)
      case ConfigurationAssignment(cmd) => runSqlHive(cmd)
      case ExplainCommand(plan) => new SharkQuery { val parsed = plan }.toString.split("\n")
      case query =>
        val result: Seq[Seq[Any]] = toRdd.collect().toSeq
        // Reformat to match hive tab delimited output.
        val asString = result.map(_.map(toHiveString)).map(_.mkString("\t")).toSeq
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

  /**
   * A shark query workflow for plans where all relations have already been resolved (likely because
   * the query was built from raw RDDs).  Additionally attribute resolution is case sensitive.
   */
  abstract class LogicalSharkQuery extends SharkQuery {
    override lazy val analyzed = SimpleAnalyzer(parsed)
  }

  implicit class stringToQuery(str: String) {
    def q = new SharkSqlQuery(str)
  }

  implicit def logicalToSharkQuery(plan: LogicalPlan) = new LogicalSharkQuery { val parsed = plan }
}