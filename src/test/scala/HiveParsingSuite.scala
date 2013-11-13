package catalyst
package frontend

import shark.SharkContext
import shark.SharkEnv

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import util.TestShark

class SQLSuite extends FunSuite with BeforeAndAfterAll {

  override def beforeAll() {
  }

  test("trivial select query") {
    val x = new TestShark
    import x._
    loadKv1
    //sc.sql("SELECT key, val FROM test").foreach(println)

    "SELECT key FROM test".q.execute().collect.foreach(println)
  }
}