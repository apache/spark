package spark.repl

import java.io.FileWriter

import org.scalatest.FunSuite

import com.google.common.io.Files

class StandaloneClusterReplSuite extends FunSuite with ReplSuiteMixin {
  val sparkUrl = "local-cluster[1,1,512]"

  test("simple collect") {
    val output = runInterpreter(sparkUrl, """
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
    val output = runInterpreter(sparkUrl, """
      val accum = sc.accumulator(0)
      sc.parallelize(1 to 10).foreach(x => accum += x)
      accum.value
      """)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res1: Int = 55", output)
  }

  test("external vars") {
    val output = runInterpreter(sparkUrl, """
      var v = 7
      sc.parallelize(1 to 10).map(x => v).take(10).reduceLeft(_+_)
      v = 10
      sc.parallelize(1 to 10).map(x => v).take(10).reduceLeft(_+_)
      """)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Int = 70", output)
    assertContains("res1: Int = 100", output)
  }

  test("external classes") {
    val output = runInterpreter(sparkUrl, """
      class C {
        def foo = 5
      }
      sc.parallelize(1 to 10).map(x => (new C).foo).take(10).reduceLeft(_+_)
      """)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Int = 50", output)
  }

  test("external functions") {
    val output = runInterpreter(sparkUrl, """
      def double(x: Int) = x + x
      sc.parallelize(1 to 10).map(x => double(x)).take(10).reduceLeft(_+_)
      """)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Int = 110", output)
  }

 test("external functions that access vars") {
   val output = runInterpreter(sparkUrl, """
     var v = 7
     def getV() = v
     sc.parallelize(1 to 10).map(x => getV()).take(10).reduceLeft(_+_)
     v = 10
     sc.parallelize(1 to 10).map(x => getV()).take(10).reduceLeft(_+_)
     """)
   assertDoesNotContain("error:", output)
   assertDoesNotContain("Exception", output)
   assertContains("res0: Int = 70", output)
   assertContains("res1: Int = 100", output)
 }

  test("broadcast vars") {
    // Test that the value that a broadcast var had when it was created is used,
    // even if that variable is then modified in the driver program

    val output = runInterpreter(sparkUrl, """
             var array = new Array[Int](5)
             val broadcastArray = sc.broadcast(array)
             sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).take(5)
             array(0) = 5
             sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).take(5)
             """)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Array[Int] = Array(0, 0, 0, 0, 0)", output)
    assertContains("res2: Array[Int] = Array(5, 0, 0, 0, 0)", output)
  }
}
