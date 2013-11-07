package catalyst
package frontend

import shark.SharkContext
import shark.SharkEnv

import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SQLSuite extends FunSuite with BeforeAndAfterAll {
  var sc: SharkContext = _

  override def beforeAll() {
    sc = TestUtils.getTestContext
  }

  test("trivial select query") {
    sc.sql("SELECT key, val FROM test").foreach(println)
  }
}