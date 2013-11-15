package catalyst
package util

import catalyst.analysis.{MetastoreRelation, Analyzer, HiveMetastoreCatalog}
import catalyst.expressions.{NamedExpression, Attribute}
import catalyst.frontend._
import catalyst.planning.{QueryPlanner, Strategy}
import catalyst.plans.logical._
import catalyst.plans.physical
import catalyst.plans.physical.PhysicalPlan
import shark.{SharkContext, SharkEnv}

import util._

class TestShark {
  val WAREHOUSE_PATH = getTempFilePath("sharkWarehouse")
  val METASTORE_PATH = getTempFilePath("sharkMetastore")
  val MASTER = "local"

  val sc = SharkEnv.initWithSharkContext("shark-sql-suite-testing", MASTER)

  sc.runSql("set javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=" + METASTORE_PATH + ";create=true")
  sc.runSql("set hive.metastore.warehouse.dir=" + WAREHOUSE_PATH)

  def loadKv1 {
    //sc.runSql("DROP TABLE IF EXISTS test")
    sc.runSql("CREATE TABLE test (key INT, val STRING)")
    // USE ENV VARS
    sc.runSql("""LOAD DATA LOCAL INPATH '/Users/marmbrus/workspace/hive/data/files/kv1.txt' INTO TABLE test""")
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

  class SharkQuery(sql: String) {
    lazy val parsed = Hive.parseSql(sql)
    lazy val analyzed = analyze(parsed)
    // TODO: Don't just pick the first one...
    lazy val physicalPlan = TrivalPlanner(analyzed).next()

    def execute() = analyzed match {
      case NativeCommand(cmd) => sc.runSql(cmd); null
      case _ => physicalPlan.execute()
    }

    override def toString(): String =
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