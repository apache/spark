package spark.repl

import org.scalatest.FunSuite

class StandaloneClusterReplSuite extends FunSuite with ReplSuiteMixin {
  setupStandaloneCluster

  test("simple collect") {
    val output = runInterpreter("spark://127.0.1.2:7089", """
      var x = 123
      val data = sc.parallelize(1 to 3).map(_ + x)
      data.take(3)
      """)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("124", output)
    assertContains("125", output)
    assertContains("126", output)
  }
  
  test("simple foreach with accumulator") {
    val output = runInterpreter("spark://127.0.1.2:7089", """
      val accum = sc.accumulator(0)
      sc.parallelize(1 to 10).foreach(x => accum += x)
      accum.value
      """)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res1: Int = 55", output)
  }

}