package catalyst
package util


import catalyst.rules.RuleExecutor
import shark.{SharkConfVars, SharkContext, SharkEnv}

import analysis.{MetastoreRelation, Analyzer, HiveMetastoreCatalog}
import expressions.{NamedExpression, Attribute}
import frontend.{Hive, NativeCommand}
import planning.{QueryPlanner, Strategy}
import plans.logical._
import plans.physical
import plans.physical.PhysicalPlan


class TestShark {
  val WAREHOUSE_PATH = getTempFilePath("sharkWarehouse")
  val METASTORE_PATH = getTempFilePath("sharkMetastore")
  val MASTER = "local"

  protected val sc = SharkEnv.initWithSharkContext("shark-sql-suite-testing", MASTER)

  // Use hive natively for queries that won't be executed by catalyst. This is because
  // shark has dependencies on a custom version of hive that we are trying to avoid
  // in catalyst.
  SharkConfVars.setVar(SharkContext.hiveconf, SharkConfVars.EXEC_MODE, "hive")
  val hiveDriver = new org.apache.hadoop.hive.ql.Driver(SharkContext.hiveconf)

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
  val analyze = new Analyzer(new HiveMetastoreCatalog(SharkContext.hiveconf))

  def planLater(plan: LogicalPlan): PhysicalPlan = TrivalPlanner(plan).next

  object DataSinks extends Strategy {
    def apply(plan: LogicalPlan): Seq[PhysicalPlan] = plan match {
      case InsertIntoHiveTable(tableName, child) =>
        physical.InsertIntoHiveTable(tableName, planLater(child))(sc) :: Nil
      case _ => Nil
    }
  }

  object HiveTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[PhysicalPlan] = plan match {
      case p @ Project(projectList, m: MetastoreRelation) if isSimpleProject(projectList) =>
        physical.HiveTableScan(projectList.asInstanceOf[Seq[Attribute]], m) :: Nil
      case m: MetastoreRelation =>
        physical.HiveTableScan(m.output, m) :: Nil
      case _ => Nil
    }

    /**
     * Returns true if [[projectList]] only performs column pruning and
     * does not evaluate other complex expressions.
     */
    def isSimpleProject(projectList: Seq[NamedExpression]) = {
      projectList.map {
        case a: Attribute => true
        case _ => false
      }.reduceLeft(_ && _)
    }
  }

  // Can we automate these 'pass through' operations?
  object BasicOperators extends Strategy {
    def apply(plan: LogicalPlan): Seq[PhysicalPlan] = plan match {
      case Sort(sortExprs, child) =>
        physical.Sort(sortExprs, planLater(child)) :: Nil
      case _ => Nil
    }
  }

  object TrivalPlanner extends QueryPlanner {
    val strategies =
      HiveTableScans ::
      DataSinks ::
      BasicOperators :: Nil
  }

  object PrepareForExecution extends RuleExecutor[PhysicalPlan] {
    val batches =
      Batch("Prepare Expressions", Once,
        expressions.BindReferences) :: Nil
  }

  class SharkQuery(sql: String) {
    lazy val parsed = Hive.parseSql(sql)
    lazy val analyzed = analyze(parsed)
    // TODO: Don't just pick the first one...
    lazy val physicalPlan = TrivalPlanner(analyzed).next()
    lazy val executedPlan = PrepareForExecution(physicalPlan)

    def execute() = analyzed match {
      case NativeCommand(cmd) => sc.sql(cmd); None
      case _ => Some(executedPlan.execute())
    }

    override def toString: String =
      s"""$sql
         |== Logical Plan ==
         |$analyzed
         |== Physical Plan ==
         |$physicalPlan
      """.stripMargin.trim
  }

  implicit class stringToQuery(str: String) {
    def q = new SharkQuery(str)
  }
}