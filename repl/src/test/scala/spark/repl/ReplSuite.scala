package spark.repl

import java.io._
import java.net.URLClassLoader

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import org.scalatest.FunSuite
import com.google.common.io.Files

class ReplSuite extends FunSuite {
  def runInterpreter(master: String, input: String): String = {
    val in = new BufferedReader(new StringReader(input + "\n"))
    val out = new StringWriter()
    val cl = getClass.getClassLoader
    var paths = new ArrayBuffer[String]
    if (cl.isInstanceOf[URLClassLoader]) {
      val urlLoader = cl.asInstanceOf[URLClassLoader]
      for (url <- urlLoader.getURLs) {
        if (url.getProtocol == "file") {
          paths += url.getFile
        }
      }
    }
    val interp = new SparkILoop(in, new PrintWriter(out), master)
    spark.repl.Main.interp = interp
    val separator = System.getProperty("path.separator")
    interp.process(Array("-classpath", paths.mkString(separator)))
    spark.repl.Main.interp = null
    if (interp.sparkContext != null)
      interp.sparkContext.stop()
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    return out.toString
  }
  
  def assertContains(message: String, output: String) {
    assert(output contains message,
           "Interpreter output did not contain '" + message + "':\n" + output)
  }
  
  def assertDoesNotContain(message: String, output: String) {
    assert(!(output contains message),
           "Interpreter output contained '" + message + "':\n" + output)
  }
  
  test ("simple foreach with accumulator") {
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

  test ("external classes") {
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

  test ("external functions") {
    val output = runInterpreter("local", """
      def double(x: Int) = x + x
      sc.parallelize(1 to 10).map(x => double(x)).collect.reduceLeft(_+_)
      """)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Int = 110", output)
  }

  test ("external functions that access vars") {
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
  
  test ("broadcast vars") {
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

  test ("interacting with files") {
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

  if (System.getenv("MESOS_NATIVE_LIBRARY") != null) {
    test ("running on Mesos") {
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
