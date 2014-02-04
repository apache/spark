/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.repl

import java.io._
import java.net.URLClassLoader

import scala.collection.mutable.ArrayBuffer

import com.google.common.io.Files
import org.scalatest.FunSuite
import org.apache.spark.SparkContext


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
    org.apache.spark.repl.Main.interp = interp
    val separator = System.getProperty("path.separator")
    interp.process(Array("-classpath", paths.mkString(separator)))
    org.apache.spark.repl.Main.interp = null
    if (interp.sparkContext != null) {
      interp.sparkContext.stop()
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    return out.toString
  }

  def assertContains(message: String, output: String) {
    assert(output.contains(message),
      "Interpreter output did not contain '" + message + "':\n" + output)
  }

  def assertDoesNotContain(message: String, output: String) {
    assert(!output.contains(message),
      "Interpreter output contained '" + message + "':\n" + output)
  }

  test("propagation of local properties") {
    // A mock ILoop that doesn't install the SIGINT handler.
    class ILoop(out: PrintWriter) extends SparkILoop(None, out, None) {
      settings = new scala.tools.nsc.Settings
      settings.usejavacp.value = true
      org.apache.spark.repl.Main.interp = this
      override def createInterpreter() {
        intp = new SparkILoopInterpreter
        intp.setContextClassLoader()
      }
    }

    val out = new StringWriter()
    val interp = new ILoop(new PrintWriter(out))
    interp.sparkContext = new SparkContext("local", "repl-test")
    interp.createInterpreter()
    interp.intp.initialize()
    interp.sparkContext.setLocalProperty("someKey", "someValue")

    // Make sure the value we set in the caller to interpret is propagated in the thread that
    // interprets the command.
    interp.interpret("org.apache.spark.repl.Main.interp.sparkContext.getLocalProperty(\"someKey\")")
    assert(out.toString.contains("someValue"))

    interp.sparkContext.stop()
    System.clearProperty("spark.driver.port")
  }

  test("simple foreach with accumulator") {
    val output = runInterpreter("local",
      """
        |val accum = sc.accumulator(0)
        |sc.parallelize(1 to 10).foreach(x => accum += x)
        |accum.value
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res1: Int = 55", output)
  }

  test("external vars") {
    val output = runInterpreter("local",
      """
        |var v = 7
        |sc.parallelize(1 to 10).map(x => v).collect.reduceLeft(_+_)
        |v = 10
        |sc.parallelize(1 to 10).map(x => v).collect.reduceLeft(_+_)
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Int = 70", output)
    assertContains("res1: Int = 100", output)
  }

  test("external classes") {
    val output = runInterpreter("local",
      """
        |class C {
        |def foo = 5
        |}
        |sc.parallelize(1 to 10).map(x => (new C).foo).collect.reduceLeft(_+_)
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Int = 50", output)
  }

  test("external functions") {
    val output = runInterpreter("local",
      """
        |def double(x: Int) = x + x
        |sc.parallelize(1 to 10).map(x => double(x)).collect.reduceLeft(_+_)
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Int = 110", output)
  }

  test("external functions that access vars") {
    val output = runInterpreter("local",
      """
        |var v = 7
        |def getV() = v
        |sc.parallelize(1 to 10).map(x => getV()).collect.reduceLeft(_+_)
        |v = 10
        |sc.parallelize(1 to 10).map(x => getV()).collect.reduceLeft(_+_)
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Int = 70", output)
    assertContains("res1: Int = 100", output)
  }

  test("broadcast vars") {
    // Test that the value that a broadcast var had when it was created is used,
    // even if that variable is then modified in the driver program
    // TODO: This doesn't actually work for arrays when we run in local mode!
    val output = runInterpreter("local",
      """
        |var array = new Array[Int](5)
        |val broadcastArray = sc.broadcast(array)
        |sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect
        |array(0) = 5
        |sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect
      """.stripMargin)
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
    val output = runInterpreter("local",
      """
        |var file = sc.textFile("%s/input").cache()
        |file.count()
        |file.count()
        |file.count()
      """.stripMargin.format(tempDir.getAbsolutePath))
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Long = 3", output)
    assertContains("res1: Long = 3", output)
    assertContains("res2: Long = 3", output)
  }

  test("local-cluster mode") {
    val output = runInterpreter("local-cluster[1,1,512]",
      """
        |var v = 7
        |def getV() = v
        |sc.parallelize(1 to 10).map(x => getV()).collect.reduceLeft(_+_)
        |v = 10
        |sc.parallelize(1 to 10).map(x => getV()).collect.reduceLeft(_+_)
        |var array = new Array[Int](5)
        |val broadcastArray = sc.broadcast(array)
        |sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect
        |array(0) = 5
        |sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Int = 70", output)
    assertContains("res1: Int = 100", output)
    assertContains("res2: Array[Int] = Array(0, 0, 0, 0, 0)", output)
    assertContains("res4: Array[Int] = Array(0, 0, 0, 0, 0)", output)
  }

  if (System.getenv("MESOS_NATIVE_LIBRARY") != null) {
    test("running on Mesos") {
      val output = runInterpreter("localquiet",
        """
          |var v = 7
          |def getV() = v
          |sc.parallelize(1 to 10).map(x => getV()).collect.reduceLeft(_+_)
          |v = 10
          |sc.parallelize(1 to 10).map(x => getV()).collect.reduceLeft(_+_)
          |var array = new Array[Int](5)
          |val broadcastArray = sc.broadcast(array)
          |sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect
          |array(0) = 5
          |sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect
        """.stripMargin)
      assertDoesNotContain("error:", output)
      assertDoesNotContain("Exception", output)
      assertContains("res0: Int = 70", output)
      assertContains("res1: Int = 100", output)
      assertContains("res2: Array[Int] = Array(0, 0, 0, 0, 0)", output)
      assertContains("res4: Array[Int] = Array(0, 0, 0, 0, 0)", output)
    }
  }
}
