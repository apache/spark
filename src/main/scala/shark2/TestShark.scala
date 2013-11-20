package catalyst
package shark2

import analysis._
import catalyst.plans.logical.LogicalPlan
import frontend.hive._
import planning._
import rules._
import shark.{SharkConfVars, SharkContext, SharkEnv}
import util._
import org.apache.spark.rdd.RDD

/**
 * A locally running test instance of spark.  The lifecycle for a given query is managed by the inner class
 * [[SharkQuery]].  A [[SharkQuery]] can either be instantiated directly or using the implicit conversion.
 *
 * {{{
 *   scala> val query = "SELECT key FROM test".q
 *   query: testShark.SharkQuery =
 *   SELECT key FROM test
 *   == Logical Plan ==
 *   Project {key#2}
 *    MetastoreRelation test
 *
 *   == Physical Plan ==
 *   HiveTableScan {key#2}, MetastoreRelation test
 *
 *   scala> query.execute().get.collect()
 *   res0: Array[IndexedSeq[Any]] = Array(Vector(238), Vector(86), Vector(311), ...
 * }}}
 */
class TestShark {
  self =>

  val WAREHOUSE_PATH = getTempFilePath("sharkWarehouse")
  val METASTORE_PATH = getTempFilePath("sharkMetastore")
  val MASTER = "local"

  protected val sc = SharkEnv.initWithSharkContext("shark-sql-suite-testing", MASTER)

  // Use hive natively for queries that won't be executed by catalyst. This is because
  // shark has dependencies on a custom version of hive that we are trying to avoid
  // in catalyst.
  SharkConfVars.setVar(SharkContext.hiveconf, SharkConfVars.EXEC_MODE, "hive")

  runSql("set javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" + METASTORE_PATH + ";create=true")
  runSql("set hive.metastore.warehouse.dir=" + WAREHOUSE_PATH)

  def runSql(sql: String) = sc.sql(sql)

  def loadKv1 {
    //sc.runSql("DROP TABLE IF EXISTS test")
    runSql("CREATE TABLE test (key INT, val STRING)")
    // USE ENV VARS
    runSql("""LOAD DATA LOCAL INPATH '/Users/marmbrus/workspace/hive/data/files/kv1.txt' INTO TABLE test""")
  }

  val catalog = new HiveMetastoreCatalog(SharkContext.hiveconf)
  val analyze = new Analyzer(catalog)

  object TrivalPlanner extends QueryPlanner[SharkPlan] with PlanningStrategies {
    val sc = self.sc
    val strategies =
      HiveTableScans ::
      DataSinks ::
      BasicOperators :: Nil
  }

  object PrepareForExecution extends RuleExecutor[SharkPlan] {
    val batches =
      Batch("Prepare Expressions", Once,
        expressions.BindReferences) :: Nil
  }


  class SharkSqlQuery(sql: String) {
    lazy val parsed = Hive.parseSql(sql)
    lazy val analyzed = analyze(parsed)
    // TODO: Don't just pick the first one...
    lazy val sharkPlan = TrivalPlanner(analyzed).next()
    lazy val executedPlan = PrepareForExecution(sharkPlan)

    def execute() = analyzed match {
      case NativeCommand(cmd) => sc.sql(cmd); None
      case _ => Some(executedPlan.execute())
    }

    override def toString: String =
      s"""$sql
         |== Logical Plan ==
         |$analyzed
         |== Physical Plan ==
         |$sharkPlan
      """.stripMargin.trim
  }

  implicit class stringToQuery(str: String) {
    def q = new SharkSqlQuery(str)
  }

  implicit class logicalToRdd(plan: LogicalPlan) {
    // TODO: Include plan info in custom rdd?
    def toRdd: RDD[IndexedSeq[Any]] = {
       val analyzed = analyze(plan)
      // TODO: Don't just pick the first one...
      val sharkPlan = TrivalPlanner(analyzed).next()
      val executedPlan = PrepareForExecution(sharkPlan)
      executedPlan.execute()
    }
  }
}