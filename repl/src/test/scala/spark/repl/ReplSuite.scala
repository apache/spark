package spark.repl

import java.io.FileWriter

import org.scalatest.FunSuite

import com.google.common.io.Files

class ReplSuite extends FunSuite with ReplSuiteMixin {

  test("simple foreach with accumulator") {
    val output = runInterpreter("local", """
      val accum = sc.accumulator(0)
      sc.parallelize(1 to 10).foreach(x => accum += x)
      accum.value
      """)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res1: Int = 55", output)
  }

  test ("external vars") {
    val output = runInterpreter("local", """
      var v = 7
      sc.parallelize(1 to 10).map(x => v).collect.reduceLeft(_+_)
      v = 10
      sc.parallelize(1 to 10).map(x => v).collect.reduceLeft(_+_)
      """)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Int = 70", output)
    assertContains("res1: Int = 100", output)
  }

  test("external classes") {
    val output = runInterpreter("local", """
      class C {
        def foo = 5
      }
      sc.parallelize(1 to 10).map(x => (new C).foo).collect.reduceLeft(_+_)
      """)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Int = 50", output)
  }

  test("external functions") {
    val output = runInterpreter("local", """
      def double(x: Int) = x + x
      sc.parallelize(1 to 10).map(x => double(x)).collect.reduceLeft(_+_)
      """)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Int = 110", output)
  }

  test("external functions that access vars") {
    val output = runInterpreter("local", """
      var v = 7
      def getV() = v
      sc.parallelize(1 to 10).map(x => getV()).collect.reduceLeft(_+_)
      v = 10
      sc.parallelize(1 to 10).map(x => getV()).collect.reduceLeft(_+_)
      """)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Int = 70", output)
    assertContains("res1: Int = 100", output)
  }

  test("broadcast vars") {
    // Test that the value that a broadcast var had when it was created is used,
    // even if that variable is then modified in the driver program
    // TODO: This doesn't actually work for arrays when we run in local mode!
    val output = runInterpreter("local", """
      var array = new Array[Int](5)
      val broadcastArray = sc.broadcast(array)
      sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect
      array(0) = 5
      sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect
      """)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Array[Int] = Array(0, 0, 0, 0, 0)", output)
    assertContains("res2: Array[Int] = Array(5, 0, 0, 0, 0)", output)
  }

  test("interacting with files") {
    val tempDir = Files.createTempDir()
    val out = new FileWriter(tempDir + "/input")
    out.write("Hello world!\n")
    out.write("What's up?\n")
    out.write("Goodbye\n")
    out.close()
    val output = runInterpreter("local", """
      var file = sc.textFile("%s/input").cache()
      file.count()
      file.count()
      file.count()
      """.format(tempDir.getAbsolutePath))
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Long = 3", output)
    assertContains("res1: Long = 3", output)
    assertContains("res2: Long = 3", output)
  }

  test ("local-cluster mode") {
    val output = runInterpreter("local-cluster[1,1,512]", """
      var v = 7
      def getV() = v
      sc.parallelize(1 to 10).map(x => getV()).collect.reduceLeft(_+_)
      v = 10
      sc.parallelize(1 to 10).map(x => getV()).collect.reduceLeft(_+_)
      var array = new Array[Int](5)
      val broadcastArray = sc.broadcast(array)
      sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect
      array(0) = 5
      sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect
      """)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Int = 70", output)
    assertContains("res1: Int = 100", output)
    assertContains("res2: Array[Int] = Array(0, 0, 0, 0, 0)", output)
    assertContains("res4: Array[Int] = Array(0, 0, 0, 0, 0)", output)
  }

  if (System.getenv("MESOS_NATIVE_LIBRARY") != null) {
    test("running on Mesos") {
      val output = runInterpreter("localquiet", """
        var v = 7
        def getV() = v
        sc.parallelize(1 to 10).map(x => getV()).collect.reduceLeft(_+_)
        v = 10
        sc.parallelize(1 to 10).map(x => getV()).collect.reduceLeft(_+_)
        var array = new Array[Int](5)
        val broadcastArray = sc.broadcast(array)
        sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect
        array(0) = 5
        sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect
        """)
      assertDoesNotContain("error:", output)
      assertDoesNotContain("Exception", output)
      assertContains("res0: Int = 70", output)
      assertContains("res1: Int = 100", output)
      assertContains("res2: Array[Int] = Array(0, 0, 0, 0, 0)", output)
      assertContains("res4: Array[Int] = Array(0, 0, 0, 0, 0)", output)
    }
  }

}
