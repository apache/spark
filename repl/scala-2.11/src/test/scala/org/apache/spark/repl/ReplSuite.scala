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
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

class ReplSuite extends SparkFunSuite {

  def runInterpreter(master: String, input: String): String = {
    val CONF_EXECUTOR_CLASSPATH = "spark.executor.extraClassPath"

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
    val classpath = paths.mkString(File.pathSeparator)

    val oldExecutorClasspath = System.getProperty(CONF_EXECUTOR_CLASSPATH)
    System.setProperty(CONF_EXECUTOR_CLASSPATH, classpath)
    Main.sparkContext = null
    Main.sparkSession = null // causes recreation of SparkContext for each test.
    Main.conf.set("spark.master", master)
    Main.doMain(Array("-classpath", classpath), new SparkILoop(in, new PrintWriter(out)))

    if (oldExecutorClasspath != null) {
      System.setProperty(CONF_EXECUTOR_CLASSPATH, oldExecutorClasspath)
    } else {
      System.clearProperty(CONF_EXECUTOR_CLASSPATH)
    }
    return out.toString
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

  test("simple foreach with accumulator") {
    val output = runInterpreter("local",
      """
        |val accum = sc.longAccumulator
        |sc.parallelize(1 to 10).foreach(x => accum.add(x))
        |accum.value
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res1: Long = 55", output)
  }

  test("external vars") {
    val output = runInterpreter("local",
      """
        |var v = 7
        |sc.parallelize(1 to 10).map(x => v).collect().reduceLeft(_+_)
        |v = 10
        |sc.parallelize(1 to 10).map(x => v).collect().reduceLeft(_+_)
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
        |sc.parallelize(1 to 10).map(x => (new C).foo).collect().reduceLeft(_+_)
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Int = 50", output)
  }

  test("external functions") {
    val output = runInterpreter("local",
      """
        |def double(x: Int) = x + x
        |sc.parallelize(1 to 10).map(x => double(x)).collect().reduceLeft(_+_)
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
        |sc.parallelize(1 to 10).map(x => getV()).collect().reduceLeft(_+_)
        |v = 10
        |sc.parallelize(1 to 10).map(x => getV()).collect().reduceLeft(_+_)
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
        |sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect()
        |array(0) = 5
        |sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Array[Int] = Array(0, 0, 0, 0, 0)", output)
    assertContains("res2: Array[Int] = Array(5, 0, 0, 0, 0)", output)
  }

  test("interacting with files") {
    val tempDir = Utils.createTempDir()
    val out = new FileWriter(tempDir + "/input")
    out.write("Hello world!\n")
    out.write("What's up?\n")
    out.write("Goodbye\n")
    out.close()
    val output = runInterpreter("local",
      """
        |var file = sc.textFile("%s").cache()
        |file.count()
        |file.count()
        |file.count()
      """.stripMargin.format(StringEscapeUtils.escapeJava(
        tempDir.getAbsolutePath + File.separator + "input")))
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res0: Long = 3", output)
    assertContains("res1: Long = 3", output)
    assertContains("res2: Long = 3", output)
    Utils.deleteRecursively(tempDir)
  }

  test("local-cluster mode") {
    val output = runInterpreter("local-cluster[1,1,1024]",
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

  test("SPARK-1199 two instances of same class don't type check.") {
    val output = runInterpreter("local",
      """
        |case class Sum(exp: String, exp2: String)
        |val a = Sum("A", "B")
        |def b(a: Sum): String = a match { case Sum(_, _) => "Found Sum" }
        |b(a)
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
  }

  test("SPARK-2452 compound statements.") {
    val output = runInterpreter("local",
      """
        |val x = 4 ; def f() = x
        |f()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
  }

  test("SPARK-2576 importing implicits") {
    // We need to use local-cluster to test this case.
    val output = runInterpreter("local-cluster[1,1,1024]",
      """
        |import spark.implicits._
        |case class TestCaseClass(value: Int)
        |sc.parallelize(1 to 10).map(x => TestCaseClass(x)).toDF().collect()
        |
        |// Test Dataset Serialization in the REPL
        |Seq(TestCaseClass(1)).toDS().collect()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
  }

  test("Datasets and encoders") {
    val output = runInterpreter("local",
      """
        |import org.apache.spark.sql.functions._
        |import org.apache.spark.sql.{Encoder, Encoders}
        |import org.apache.spark.sql.expressions.Aggregator
        |import org.apache.spark.sql.TypedColumn
        |val simpleSum = new Aggregator[Int, Int, Int] {
        |  def zero: Int = 0                     // The initial value.
        |  def reduce(b: Int, a: Int) = b + a    // Add an element to the running total
        |  def merge(b1: Int, b2: Int) = b1 + b2 // Merge intermediate values.
        |  def finish(b: Int) = b                // Return the final result.
        |  def bufferEncoder: Encoder[Int] = Encoders.scalaInt
        |  def outputEncoder: Encoder[Int] = Encoders.scalaInt
        |}.toColumn
        |
        |val ds = Seq(1, 2, 3, 4).toDS()
        |ds.select(simpleSum).collect
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
  }

  test("SPARK-2632 importing a method from non serializable class and not using it.") {
    val output = runInterpreter("local-cluster[1,1,1024]",
      """
      |class TestClass() { def testMethod = 3 }
      |val t = new TestClass
      |import t.testMethod
      |case class TestCaseClass(value: Int)
      |sc.parallelize(1 to 10).map(x => TestCaseClass(x)).collect()
    """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
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

  test("collecting objects of class defined in repl") {
    val output = runInterpreter("local[2]",
      """
        |case class Foo(i: Int)
        |val ret = sc.parallelize((1 to 100).map(Foo), 10).collect()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("ret: Array[Foo] = Array(Foo(1),", output)
  }

  test("collecting objects of class defined in repl - shuffling") {
    val output = runInterpreter("local-cluster[1,1,1024]",
      """
        |case class Foo(i: Int)
        |val list = List((1, Foo(1)), (1, Foo(2)))
        |val ret = sc.parallelize(list).groupByKey().collect()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("ret: Array[(Int, Iterable[Foo])] = Array((1,", output)
  }

  test("replicating blocks of object with class defined in repl") {
    val output = runInterpreter("local-cluster[2,1,1024]",
      """
        |val timeout = 60000 // 60 seconds
        |val start = System.currentTimeMillis
        |while(sc.getExecutorStorageStatus.size != 3 &&
        |    (System.currentTimeMillis - start) < timeout) {
        |  Thread.sleep(10)
        |}
        |if (System.currentTimeMillis - start >= timeout) {
        |  throw new java.util.concurrent.TimeoutException("Executors were not up in 60 seconds")
        |}
        |import org.apache.spark.storage.StorageLevel._
        |case class Foo(i: Int)
        |val ret = sc.parallelize((1 to 100).map(Foo), 10).persist(MEMORY_AND_DISK_2)
        |ret.count()
        |sc.getExecutorStorageStatus.map(s => s.rddBlocksById(ret.id).size).sum
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains(": Int = 20", output)
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

  test("should clone and clean line object in ClosureCleaner") {
    val output = runInterpreterInPasteMode("local-cluster[1,4,4096]",
      """
        |import org.apache.spark.rdd.RDD
        |
        |val lines = sc.textFile("pom.xml")
        |case class Data(s: String)
        |val dataRDD = lines.map(line => Data(line.take(3)))
        |dataRDD.cache.count
        |val repartitioned = dataRDD.repartition(dataRDD.partitions.size)
        |repartitioned.cache.count
        |
        |def getCacheSize(rdd: RDD[_]) = {
        |  sc.getRDDStorageInfo.filter(_.id == rdd.id).map(_.memSize).sum
        |}
        |val cacheSize1 = getCacheSize(dataRDD)
        |val cacheSize2 = getCacheSize(repartitioned)
        |
        |// The cache size of dataRDD and the repartitioned one should be similar.
        |val deviation = math.abs(cacheSize2 - cacheSize1).toDouble / cacheSize1
        |assert(deviation < 0.2,
        |  s"deviation too large: $deviation, first size: $cacheSize1, second size: $cacheSize2")
      """.stripMargin)
    assertDoesNotContain("AssertionError", output)
    assertDoesNotContain("Exception", output)
  }
}
