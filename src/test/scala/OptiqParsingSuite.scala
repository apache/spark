package catalyst
package frontend

import org.scalatest.{FunSuite}

class OptiqParsingSuite extends FunSuite {
  test("simple select") {
    println(Optiq.parseSql("SELECT a FROM foo"))
    println(Optiq.parseSql("SELECT a AS b FROM foo"))
    println(Optiq.parseSql("SELECT a AS b FROM foo JOIN bar"))
  }
}
