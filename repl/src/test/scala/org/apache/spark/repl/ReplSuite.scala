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

import scala.tools.nsc.interpreter.SimpleReader

import org.apache.log4j.{Level, LogManager}

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

class ReplSuite extends SparkFunSuite {

  def runInterpreter(master: String, input: String): String = {
    val CONF_EXECUTOR_CLASSPATH = "spark.executor.extraClassPath"

    val oldExecutorClasspath = System.getProperty(CONF_EXECUTOR_CLASSPATH)
    val classpath = System.getProperty("java.class.path")
    System.setProperty(CONF_EXECUTOR_CLASSPATH, classpath)

    Main.sparkContext = null
    Main.sparkSession = null // causes recreation of SparkContext for each test.
    Main.conf.set("spark.master", master)

    val in = new BufferedReader(new StringReader(input + "\n"))
    val out = new StringWriter()
    Main.doMain(Array("-classpath", classpath), new SparkILoop(in, new PrintWriter(out)))

    if (oldExecutorClasspath != null) {
      System.setProperty(CONF_EXECUTOR_CLASSPATH, oldExecutorClasspath)
    } else {
      System.clearProperty(CONF_EXECUTOR_CLASSPATH)
    }

    out.toString
  }

  // Simulate the paste mode in Scala REPL.
  def runInterpreterInPasteMode(master: String, input: String): String =
    runInterpreter(master, ":paste\n" + input + 4.toChar) // 4 is the ascii code of CTRL + D

  def assertContains(message: String, output: String) {
    val isContain = output.contains(message)
    assert(isContain,
      "Interpreter output did not contain '" + message + "':\n" + output)
  }

  def assertDoesNotContain(message: String, output: String) {
    val isContain = output.contains(message)
    assert(!isContain,
      "Interpreter output contained '" + message + "':\n" + output)
  }

  test("propagation of local properties") {
    // A mock ILoop that doesn't install the SIGINT handler.
    class ILoop(out: PrintWriter) extends SparkILoop(None, out) {
      settings = new scala.tools.nsc.Settings
      settings.usejavacp.value = true
      org.apache.spark.repl.Main.interp = this
      in = SimpleReader()
    }

    val out = new StringWriter()
    Main.interp = new ILoop(new PrintWriter(out))
    Main.sparkContext = new SparkContext("local", "repl-test")
    Main.interp.createInterpreter()

    Main.sparkContext.setLocalProperty("someKey", "someValue")

    // Make sure the value we set in the caller to interpret is propagated in the thread that
    // interprets the command.
    Main.interp.interpret("org.apache.spark.repl.Main.sparkContext.getLocalProperty(\"someKey\")")
    assert(out.toString.contains("someValue"))

    Main.sparkContext.stop()
    System.clearProperty("spark.driver.port")
  }

  test("SPARK-15236: use Hive catalog") {
    // turn on the INFO log so that it is possible the code will dump INFO
    // entry for using "HiveMetastore"
    val rootLogger = LogManager.getRootLogger()
    val logLevel = rootLogger.getLevel
    rootLogger.setLevel(Level.INFO)
    try {
      Main.conf.set(CATALOG_IMPLEMENTATION.key, "hive")
      val output = runInterpreter("local",
        """
      |spark.sql("drop table if exists t_15236")
    """.stripMargin)
      assertDoesNotContain("error:", output)
      assertDoesNotContain("Exception", output)
      // only when the config is set to hive and
      // hive classes are built, we will use hive catalog.
      // Then log INFO entry will show things using HiveMetastore
      if (SparkSession.hiveClassesArePresent) {
        assertContains("HiveMetaStore", output)
      } else {
        // If hive classes are not built, in-memory catalog will be used
        assertDoesNotContain("HiveMetaStore", output)
      }
    } finally {
      rootLogger.setLevel(logLevel)
    }
  }

  test("SPARK-15236: use in-memory catalog") {
    val rootLogger = LogManager.getRootLogger()
    val logLevel = rootLogger.getLevel
    rootLogger.setLevel(Level.INFO)
    try {
      Main.conf.set(CATALOG_IMPLEMENTATION.key, "in-memory")
      val output = runInterpreter("local",
        """
          |spark.sql("drop table if exists t_16236")
        """.stripMargin)
      assertDoesNotContain("error:", output)
      assertDoesNotContain("Exception", output)
      assertDoesNotContain("HiveMetaStore", output)
    } finally {
      rootLogger.setLevel(logLevel)
    }
  }

  test("broadcast vars") {
    // Test that the value that a broadcast var had when it was created is used,
    // even if that variable is then modified in the driver program
    // TODO: This doesn't actually work for arrays when we run in local mode!
    val output = runInterpreter("local",
      """
        |var array = new Array[Int](5)
        |val broadcastArray = sc.broadcast(array)
        |sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect()
        |array(0) = 5
        |sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Array[Int] = Array(0, 0, 0, 0, 0)", output)
    assertContains("res2: Array[Int] = Array(5, 0, 0, 0, 0)", output)
  }

  if (System.getenv("MESOS_NATIVE_JAVA_LIBRARY") != null) {
    test("running on Mesos") {
      val output = runInterpreter("localquiet",
        """
          |var v = 7
          |def getV() = v
          |sc.parallelize(1 to 10).map(x => getV()).collect().reduceLeft(_+_)
          |v = 10
          |sc.parallelize(1 to 10).map(x => getV()).collect().reduceLeft(_+_)
          |var array = new Array[Int](5)
          |val broadcastArray = sc.broadcast(array)
          |sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect()
          |array(0) = 5
          |sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect()
        """.stripMargin)
      assertDoesNotContain("error:", output)
      assertDoesNotContain("Exception", output)
      assertContains("res0: Int = 70", output)
      assertContains("res1: Int = 100", output)
      assertContains("res2: Array[Int] = Array(0, 0, 0, 0, 0)", output)
      assertContains("res4: Array[Int] = Array(0, 0, 0, 0, 0)", output)
    }
  }

  test("line wrapper only initialized once when used as encoder outer scope") {
    val output = runInterpreter("local",
      """
        |val fileName = "repl-test-" + System.currentTimeMillis
        |val tmpDir = System.getProperty("java.io.tmpdir")
        |val file = new java.io.File(tmpDir, fileName)
        |def createFile(): Unit = file.createNewFile()
        |
        |createFile();case class TestCaseClass(value: Int)
        |sc.parallelize(1 to 10).map(x => TestCaseClass(x)).collect()
        |
        |file.delete()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
  }

  test("define case class and create Dataset together with paste mode") {
    val output = runInterpreterInPasteMode("local-cluster[1,1,1024]",
      """
        |import spark.implicits._
        |case class TestClass(value: Int)
        |Seq(TestClass(1)).toDS()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
  }

  test(":replay should work correctly") {
   val output = runInterpreter("local",
     """
     |sc
     |:replay
     """.stripMargin)
    assertDoesNotContain("error: not found: value sc", output)
  }

  test("spark-shell should find imported types in class constructors and extends clause") {
    val output = runInterpreter("local",
      """
        |import org.apache.spark.Partition
        |class P(p: Partition)
        |class P(val index: Int) extends Partition
      """.stripMargin)
    assertDoesNotContain("error: not found: type Partition", output)
  }

  test("spark-shell should shadow val/def definitions correctly") {
    val output1 = runInterpreter("local",
      """
        |def myMethod() = "first definition"
        |val tmp = myMethod(); val out = tmp
        |def myMethod() = "second definition"
        |val tmp = myMethod(); val out = s"$tmp aabbcc"
      """.stripMargin)
    assertContains("second definition aabbcc", output1)

    val output2 = runInterpreter("local",
      """
        |val a = 1
        |val b = a; val c = b;
        |val a = 2
        |val b = a; val c = b;
        |s"!!$b!!"
      """.stripMargin)
    assertContains("!!2!!", output2)
  }

  test("SPARK-26633: ExecutorClassLoader.getResourceAsStream find REPL classes") {
    val output = runInterpreterInPasteMode("local-cluster[1,1,1024]",
      """
        |case class TestClass(value: Int)
        |
        |sc.parallelize(1 to 1).map { _ =>
        |  val clz = classOf[TestClass]
        |  val name = clz.getName.replace('.', '/') + ".class";
        |  val stream = clz.getClassLoader.getResourceAsStream(name)
        |  if (stream == null) {
        |    "failed: stream is null"
        |  } else {
        |    val magic = new Array[Byte](4)
        |    try {
        |      stream.read(magic)
        |      // the magic number of a Java Class file
        |      val expected = Array[Byte](0xCA.toByte, 0xFE.toByte, 0xBA.toByte, 0xBE.toByte)
        |      if (magic sameElements expected) {
        |        "successful"
        |      } else {
        |        "failed: unexpected contents from stream"
        |      }
        |    } finally {
        |      stream.close()
        |    }
        |  }
        |}.collect()
      """.stripMargin)
    assertDoesNotContain("failed", output)
    assertContains("successful", output)
  }

}
