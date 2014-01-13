package catalyst
package execution

import org.scalatest.{FunSuite, BeforeAndAfterAll}

class ConcurrentHiveTests extends FunSuite with BeforeAndAfterAll {
  test("Multiple Hive Instances") {
    (1 to 10).map { i =>
      val ts = new TestSharkInstance
      ts.runSqlHive("SHOW TABLES")
      val q = ts.stringToTestQuery("SELECT * FROM src").q
      q.toRdd.collect()
      ts.runSqlHive("SHOW TABLES")
    }
  }
}