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

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

/**
 * A special test suite for REPL that all test cases share one REPL instance.
 */
class SingletonReplSuite extends SparkFunSuite {

  private val out = new StringWriter()
  private val in = new PipedOutputStream()
  private var thread: Thread = _

  private val CONF_EXECUTOR_CLASSPATH = "spark.executor.extraClassPath"
  private val oldExecutorClasspath = System.getProperty(CONF_EXECUTOR_CLASSPATH)

  override def beforeAll(): Unit = {
    super.beforeAll()

    val classpath = System.getProperty("java.class.path")
    System.setProperty(CONF_EXECUTOR_CLASSPATH, classpath)

    Main.conf.set("spark.master", "local-cluster[2,1,1024]")
    val interp = new SparkILoop(
      new BufferedReader(new InputStreamReader(new PipedInputStream(in))),
      new PrintWriter(out))

    // Forces to create new SparkContext
    Main.sparkContext = null
    Main.sparkSession = null

    // Starts a new thread to run the REPL interpreter, so that we won't block.
    thread = new Thread(() => Main.doMain(Array("-classpath", classpath), interp))
    thread.setDaemon(true)
    thread.start()

    waitUntil(() => out.toString.contains("Type :help for more information"))
  }

  override def afterAll(): Unit = {
    in.close()
    thread.join()
    if (oldExecutorClasspath != null) {
      System.setProperty(CONF_EXECUTOR_CLASSPATH, oldExecutorClasspath)
    } else {
      System.clearProperty(CONF_EXECUTOR_CLASSPATH)
    }
    super.afterAll()
  }

  private def waitUntil(cond: () => Boolean): Unit = {
    import scala.concurrent.duration._
    import org.scalatest.concurrent.Eventually._

    eventually(timeout(50.seconds), interval(500.millis)) {
      assert(cond(), "current output: " + out.toString)
    }
  }

  /**
   * Run the given commands string in a globally shared interpreter instance. Note that the given
   * commands should not crash the interpreter, to not affect other test cases.
   */
  def runInterpreter(input: String): String = {
    val currentOffset = out.getBuffer.length()
    // append a special statement to the end of the given code, so that we can know what's
    // the final output of this code snippet and rely on it to wait until the output is ready.
    val timestamp = System.currentTimeMillis()
    in.write((input + s"\nval _result_$timestamp = 1\n").getBytes)
    in.flush()
    val stopMessage = s"_result_$timestamp: Int = 1"
    waitUntil(() => out.getBuffer.substring(currentOffset).contains(stopMessage))
    out.getBuffer.substring(currentOffset)
  }

  def assertContains(message: String, output: String): Unit = {
    val isContain = output.contains(message)
    assert(isContain,
      "Interpreter output did not contain '" + message + "':\n" + output)
  }

  def assertDoesNotContain(message: String, output: String): Unit = {
    val isContain = output.contains(message)
    assert(!isContain,
      "Interpreter output contained '" + message + "':\n" + output)
  }

  test("simple foreach with accumulator") {
    val output = runInterpreter(
      """
        |val accum = sc.longAccumulator
        |sc.parallelize(1 to 10).foreach(x => accum.add(x))
        |val res = accum.value
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res: Long = 55", output)
  }

  test("external vars") {
    val output = runInterpreter(
      """
        |var v = 7
        |val res1 = sc.parallelize(1 to 10).map(x => v).collect().reduceLeft(_+_)
        |v = 10
        |val res2 = sc.parallelize(1 to 10).map(x => v).collect().reduceLeft(_+_)
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res1: Int = 70", output)
    assertContains("res2: Int = 100", output)
  }

  test("external classes") {
    val output = runInterpreter(
      """
        |class C {
        |def foo = 5
        |}
        |val res = sc.parallelize(1 to 10).map(x => (new C).foo).collect().reduceLeft(_+_)
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res: Int = 50", output)
  }

  test("external functions") {
    val output = runInterpreter(
      """
        |def double(x: Int) = x + x
        |val res = sc.parallelize(1 to 10).map(x => double(x)).collect().reduceLeft(_+_)
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res: Int = 110", output)
  }

  test("external functions that access vars") {
    val output = runInterpreter(
      """
        |var v = 7
        |def getV() = v
        |val res1 = sc.parallelize(1 to 10).map(x => getV()).collect().reduceLeft(_+_)
        |v = 10
        |val res2 = sc.parallelize(1 to 10).map(x => getV()).collect().reduceLeft(_+_)
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res1: Int = 70", output)
    assertContains("res2: Int = 100", output)
  }

  test("broadcast vars") {
    // Test that the value that a broadcast var had when it was created is used,
    // even if that variable is then modified in the driver program
    val output = runInterpreter(
      """
        |var array = new Array[Int](5)
        |val broadcastArray = sc.broadcast(array)
        |val res1 = sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect()
        |array(0) = 5
        |val res2 = sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res1: Array[Int] = Array(0, 0, 0, 0, 0)", output)
    assertContains("res2: Array[Int] = Array(0, 0, 0, 0, 0)", output)
  }

  test("interacting with files") {
    val tempDir = Utils.createTempDir()
    val out = new FileWriter(tempDir + "/input")
    out.write("Hello world!\n")
    out.write("What's up?\n")
    out.write("Goodbye\n")
    out.close()
    val output = runInterpreter(
      """
        |var file = sc.textFile("%s").cache()
        |val res1 = file.count()
        |val res2 = file.count()
        |val res3 = file.count()
      """.stripMargin.format(StringEscapeUtils.escapeJava(
        tempDir.getAbsolutePath + File.separator + "input")))
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res1: Long = 3", output)
    assertContains("res2: Long = 3", output)
    assertContains("res3: Long = 3", output)
    Utils.deleteRecursively(tempDir)
  }

  test("local-cluster mode") {
    val output = runInterpreter(
      """
        |var v = 7
        |def getV() = v
        |val res1 = sc.parallelize(1 to 10).map(x => getV()).collect().reduceLeft(_+_)
        |v = 10
        |val res2 = sc.parallelize(1 to 10).map(x => getV()).collect().reduceLeft(_+_)
        |var array = new Array[Int](5)
        |val broadcastArray = sc.broadcast(array)
        |val res3 = sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect()
        |array(0) = 5
        |val res4 = sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res1: Int = 70", output)
    assertContains("res2: Int = 100", output)
    assertContains("res3: Array[Int] = Array(0, 0, 0, 0, 0)", output)
    assertContains("res4: Array[Int] = Array(0, 0, 0, 0, 0)", output)
  }

  test("SPARK-1199 two instances of same class don't type check.") {
    val output = runInterpreter(
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
    val output = runInterpreter(
      """
        |val x = 4 ; def f() = x
        |f()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
  }

  test("SPARK-2576 importing implicits") {
    // We need to use local-cluster to test this case.
    val output = runInterpreter(
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
    val output = runInterpreter(
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
    val output = runInterpreter(
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

  test("collecting objects of class defined in repl") {
    val output = runInterpreter(
      """
        |case class Foo(i: Int)
        |val res = sc.parallelize((1 to 100).map(Foo), 10).collect()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res: Array[Foo] = Array(Foo(1),", output)
  }

  test("collecting objects of class defined in repl - shuffling") {
    val output = runInterpreter(
      """
        |case class Foo(i: Int)
        |val list = List((1, Foo(1)), (1, Foo(2)))
        |val res = sc.parallelize(list).groupByKey().collect()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res: Array[(Int, Iterable[Foo])] = Array((1,", output)
  }

  test("replicating blocks of object with class defined in repl") {
    val output = runInterpreter(
      """
        |val timeout = 60000 // 60 seconds
        |val start = System.currentTimeMillis
        |while(sc.statusTracker.getExecutorInfos.size != 3 &&
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
        |val res = sc.getRDDStorageInfo.filter(_.id == ret.id).map(_.numCachedPartitions).sum
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res: Int = 10", output)
  }

  test("should clone and clean line object in ClosureCleaner") {
    val output = runInterpreter(
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

  test("newProductSeqEncoder with REPL defined class") {
    val output = runInterpreter(
      """
        |case class Click(id: Int)
        |spark.implicits.newProductSeqEncoder[Click]
      """.stripMargin)

    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
  }

  test("create encoder in executors") {
    val output = runInterpreter(
      """
        |case class Foo(s: String)
        |
        |import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
        |
        |val r =
        |  sc.parallelize(1 to 1).map { i => ExpressionEncoder[Foo](); Foo("bar") }.collect.head
      """.stripMargin)

    assertContains("r: Foo = Foo(bar)", output)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
  }
}
