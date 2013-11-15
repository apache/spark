package catalyst
package util

import catalyst.analysis.{Analyzer, HiveMetastoreCatalog}
import catalyst.frontend._
import catalyst.planning.TrivalPlanner
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
    sc.runSql("""LOAD DATA LOCAL INPATH '/Users/marmbrus/workspace/hive/data/files/kv1.txt' INTO TABLE test""")
  }

  val catalog = new HiveMetastoreCatalog(SharkContext.hiveconf)
  val analyze = new Analyzer(new HiveMetastoreCatalog(SharkContext.hiveconf))

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