package catalyst
package frontend

import shark.SharkContext
import shark.SharkEnv

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import util.TestShark

class SQLSuite extends FunSuite with BeforeAndAfterAll {
  var sc: SharkContext = _

  override def beforeAll() {
    val testShark = new TestShark
    testShark.loadKv1
    sc = testShark.sc
  }

  test("trivial select query") {
    sc.sql("SELECT key, val FROM test").foreach(println)
  }
}