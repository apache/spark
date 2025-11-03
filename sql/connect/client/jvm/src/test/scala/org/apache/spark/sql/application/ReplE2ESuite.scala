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
package org.apache.spark.sql.application

import java.io.{PipedInputStream, PipedOutputStream}
import java.nio.file.Paths
import java.util.concurrent.{Executors, Semaphore, TimeUnit}

import scala.util.Properties

import org.apache.commons.io.output.ByteArrayOutputStream
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.connect.test.{ConnectFunSuite, IntegrationTestUtils, RemoteSparkSession}
import org.apache.spark.tags.AmmoniteTest
import org.apache.spark.util.IvyTestUtils
import org.apache.spark.util.MavenUtils.MavenCoordinate

@AmmoniteTest
class ReplE2ESuite extends ConnectFunSuite with RemoteSparkSession with BeforeAndAfterEach {

  private val executorService = Executors.newSingleThreadExecutor()
  private val TIMEOUT_SECONDS = 30

  private var testSuiteOut: PipedOutputStream = _
  private var ammoniteOut: ByteArrayOutputStream = _
  private var errorStream: ByteArrayOutputStream = _
  private var ammoniteIn: PipedInputStream = _
  private val semaphore: Semaphore = new Semaphore(0)

  private val scalaVersion = Properties.versionNumberString
    .split("\\.")
    .take(2)
    .mkString(".")

  private def getCleanString(out: ByteArrayOutputStream): String = {
    // Remove ANSI colour codes
    // Regex taken from https://stackoverflow.com/a/25189932
    out.toString("UTF-8").replaceAll("\u001B\\[[\\d;]*[^\\d;]", "")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    ammoniteOut = new ByteArrayOutputStream()
    testSuiteOut = new PipedOutputStream()
    // Connect the `testSuiteOut` and `ammoniteIn` pipes
    ammoniteIn = new PipedInputStream(testSuiteOut)
    errorStream = new ByteArrayOutputStream()

    val args = Array("--port", serverPort.toString)
    val task = new Runnable {
      override def run(): Unit = {
        System.setProperty("spark.sql.abc", "abc")
        ConnectRepl.doMain(
          args = args,
          semaphore = Some(semaphore),
          inputStream = ammoniteIn,
          outputStream = ammoniteOut,
          errorStream = errorStream)
      }
    }

    executorService.submit(task)
  }

  override def afterAll(): Unit = {
    executorService.shutdownNow()
    super.afterAll()
  }

  override def afterEach(): Unit = {
    semaphore.drainPermits()
  }

  def runCommandsInShell(input: String): String = {
    ammoniteOut.reset()
    require(input.nonEmpty)
    // Pad the input with a semaphore release so that we know when the execution of the provided
    // input is complete.
    val paddedInput = input + '\n' + "semaphore.release()\n"
    testSuiteOut.write(paddedInput.getBytes)
    testSuiteOut.flush()
    if (!semaphore.tryAcquire(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
      val failOut = getCleanString(ammoniteOut)
      val errOut = getCleanString(errorStream)
      val errorString =
        s"""
          |REPL Timed out while running command: $input
          |Console output: $failOut
          |Error output: $errOut
          |""".stripMargin
      throw new RuntimeException(errorString)
    }
    getCleanString(ammoniteOut)
  }

  def runCommandsUsingSingleCellInShell(input: String): String = {
    runCommandsInShell("{\n" + input + "\n}")
  }

  def assertContains(message: String, output: String): Unit = {
    val isContain = output.contains(message)
    assert(
      isContain,
      "Ammonite output did not contain '" + message + "':\n" + output +
        s"\nError Output: ${getCleanString(errorStream)}")
  }

  test("Simple query") {
    // Run simple query to test REPL
    val input = """
        |spark.sql("select 1").collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[org.apache.spark.sql.Row] = Array([1])", output)
  }

  test("UDF containing 'def'") {
    val input = """
        |class A(x: Int) { def get = x * 5 + 19 }
        |def dummyUdf(x: Int): Int = new A(x).get
        |val myUdf = udf(dummyUdf _)
        |spark.range(5).select(myUdf(col("id"))).as[Int].collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[Int] = Array(19, 24, 29, 34, 39)", output)
  }

  test("UDF containing in-place lambda") {
    val input = """
        |class A(x: Int) { def get = x * 42 + 5 }
        |val myUdf = udf((x: Int) => new A(x).get)
        |spark.range(5).select(myUdf(col("id"))).as[Int].collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[Int] = Array(5, 47, 89, 131, 173)", output)
  }

  test("Updating UDF properties") {
    val input = """
        |class A(x: Int) { def get = x * 7 }
        |val myUdf = udf((x: Int) => new A(x).get)
        |val modifiedUdf = myUdf.withName("myUdf").asNondeterministic()
        |spark.range(5).select(modifiedUdf(col("id"))).as[Int].collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[Int] = Array(0, 7, 14, 21, 28)", output)
  }

  test("SPARK-43198: Filter does not throw ammonite-related class initialization exception") {
    val input = """
        |spark.range(10).filter(n => n % 2 == 0).collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[java.lang.Long] = Array(0L, 2L, 4L, 6L, 8L)", output)
  }

  test("Client-side JAR") {
    // scalastyle:off classforname line.size.limit
    val sparkHome = IntegrationTestUtils.sparkHome
    val testJar = Paths
      .get(s"$sparkHome/sql/connect/client/jvm/src/test/resources/TestHelloV2_$scalaVersion.jar")
      .toFile

    assume(testJar.exists(), "Missing TestHelloV2 jar!")
    val input = s"""
        |import java.nio.file.Paths
        |def classLoadingTest(x: Int): Int = {
        |  val classloader =
        |    Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
        |  val cls = Class.forName("com.example.Hello$$", true, classloader)
        |  val module = cls.getField("MODULE$$").get(null)
        |  cls.getMethod("test").invoke(module).asInstanceOf[Int]
        |}
        |val classLoaderUdf = udf(classLoadingTest _)
        |
        |val jarPath = Paths.get("${testJar.toString}").toUri
        |spark.addArtifact(jarPath)
        |
        |spark.range(5).select(classLoaderUdf(col("id"))).as[Int].collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[Int] = Array(2, 2, 2, 2, 2)", output)
    // scalastyle:on classforname line.size.limit
  }

  test("External JAR") {
    val main = MavenCoordinate("my.great.lib", "mylib", "0.1")
    IvyTestUtils.withRepository(main, None, None) { repo =>
      val input =
        s"""
           |// this import will fail
           |import my.great.lib.MyLib
           |
           |// making library available in the REPL to compile UDF
           |import coursierapi.{Credentials, MavenRepository}
           |interp.repositories() ++= Seq(MavenRepository.of("$repo"))
           |import $$ivy.`my.great.lib:mylib:0.1`
           |
           |val func = udf((a: Int) => {
           |  import my.great.lib.MyLib
           |  MyLib.myFunc(a)
           |})
           |
           |// add library to the Executor
           |spark.addArtifact("ivy://my.great.lib:mylib:0.1?repos=$repo")
           |
           |spark.range(5).select(func(col("id"))).as[Int].collect()
           |""".stripMargin
      val output = runCommandsInShell(input)
      // making sure the library was not available before installation
      assertContains("not found: value my", getCleanString(errorStream))
      assertContains("Array[Int] = Array(1, 2, 3, 4, 5)", output)
    }
  }

  test("Java UDF") {
    val input =
      """
        |import org.apache.spark.sql.api.java._
        |import org.apache.spark.sql.types.LongType
        |
        |val javaUdf = udf(new UDF1[Long, Long] {
        |  override def call(num: Long): Long = num * num + 25L
        |}, LongType).asNondeterministic()
        |spark.range(5).select(javaUdf(col("id"))).as[Long].collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[Long] = Array(25L, 26L, 29L, 34L, 41L)", output)
  }

  test("Java UDF Registration") {
    val input =
      """
        |import org.apache.spark.sql.api.java._
        |import org.apache.spark.sql.types.LongType
        |
        |spark.udf.register("javaUdf", new UDF1[Long, Long] {
        |  override def call(num: Long): Long = num * num * num + 250L
        |}, LongType)
        |spark.sql("select javaUdf(id) from range(5)").as[Long].collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[Long] = Array(250L, 251L, 258L, 277L, 314L)", output)
  }

  test("UDF Registration") {
    val input = """
        |class A(x: Int) { def get = x * 100 }
        |val myUdf = udf((x: Int) => new A(x).get)
        |spark.udf.register("dummyUdf", myUdf)
        |spark.sql("select dummyUdf(id) from range(5)").as[Long].collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[Long] = Array(0L, 100L, 200L, 300L, 400L)", output)
  }

  test("UDF closure registration") {
    val input = """
        |class A(x: Int) { def get = x * 15 }
        |spark.udf.register("directUdf", (x: Int) => new A(x).get)
        |spark.sql("select directUdf(id) from range(5)").as[Long].collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[Long] = Array(0L, 15L, 30L, 45L, 60L)", output)
  }

  test("call_udf") {
    val input = """
        |val df = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
        |spark.udf.register("simpleUDF", (v: Int) => v * v)
        |df.select($"id", call_udf("simpleUDF", $"value")).collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[org.apache.spark.sql.Row] = Array([id1,1], [id2,16], [id3,25])", output)
  }

  test("call_function") {
    val input = """
        |val df = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
        |spark.udf.register("simpleUDF", (v: Int) => v * v)
        |df.select($"id", call_function("simpleUDF", $"value")).collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[org.apache.spark.sql.Row] = Array([id1,1], [id2,16], [id3,25])", output)
  }

  test("Single Cell Compilation") {
    val input =
      """
        |case class C1(value: Int)
        |case class C2(value: Int)
        |val h1 = classOf[C1].getDeclaringClass
        |val h2 = classOf[C2].getDeclaringClass
        |val same = h1 == h2
        |""".stripMargin
    assertContains("same: Boolean = false", runCommandsInShell(input))
    assertContains("same: Boolean = true", runCommandsUsingSingleCellInShell(input))
  }

  test("Local relation containing REPL generated class") {
    val input =
      """
        |case class MyTestClass(value: Int)
        |val data = (0 to 10).map(MyTestClass)
        |spark.createDataset(data).map(mtc => mtc.value).select(sum($"value")).as[Long].head
        |""".stripMargin
    val expected = "Long = 55L"
    assertContains(expected, runCommandsInShell(input))
    assertContains(expected, runCommandsUsingSingleCellInShell(input))
  }

  test("Collect REPL generated class") {
    val input =
      """
        |case class MyTestClass(value: Int)
        |spark.range(4).
        |  filter($"id" % 2 === 1).
        |  select($"id".cast("int").as("value")).
        |  as[MyTestClass].
        |  collect().
        |  map(mtc => s"MyTestClass(${mtc.value})").
        |  mkString("[", ", ", "]")
          """.stripMargin
    val expected = """String = "[MyTestClass(1), MyTestClass(3)]""""
    assertContains(expected, runCommandsInShell(input))
    assertContains(expected, runCommandsUsingSingleCellInShell(input))
  }

  test("REPL class in encoder") {
    val input = """
        |case class MyTestClass(value: Int)
        |spark.range(3).
        |  select(col("id").cast("int").as("value")).
        |  as[MyTestClass].
        |  map(mtc => mtc.value).
        |  collect()
      """.stripMargin
    val expected = "Array[Int] = Array(0, 1, 2)"
    assertContains(expected, runCommandsInShell(input))
    assertContains(expected, runCommandsUsingSingleCellInShell(input))
  }

  test("REPL class in UDF") {
    val input = """
        |case class MyTestClass(value: Int)
        |spark.range(2).
        |  map(i => MyTestClass(i.toInt)).
        |  collect().
        |  map(mtc => s"MyTestClass(${mtc.value})").
        |  mkString("[", ", ", "]")
      """.stripMargin
    val expected = """String = "[MyTestClass(0), MyTestClass(1)]""""
    assertContains(expected, runCommandsInShell(input))
    assertContains(expected, runCommandsUsingSingleCellInShell(input))
  }

  test("streaming works with REPL generated code") {
    val input =
      """
        |val add1 = udf((i: Long) => i + 1)
        |val query = {
        |  spark.readStream
        |      .format("rate")
        |      .option("rowsPerSecond", "10")
        |      .option("numPartitions", "1")
        |      .load()
        |      .withColumn("value", add1($"value"))
        |      .writeStream
        |      .format("memory")
        |      .queryName("my_sink")
        |      .start()
        |}
        |var progress = query.lastProgress
        |while (query.isActive && (progress == null || progress.numInputRows == 0)) {
        |  query.awaitTermination(100)
        |  progress = query.lastProgress
        |}
        |val noException = query.exception.isEmpty
        |query.stop()
        |""".stripMargin
    val output = runCommandsInShell(input)
    assertContains("noException: Boolean = true", output)
  }

  test("broadcast works with REPL generated code") {
    val input =
      """
        |val add1 = udf((i: Long) => i + 1)
        |val tableA = spark.range(2).alias("a")
        |val tableB = broadcast(spark.range(2).select(add1(col("id")).alias("id"))).alias("b")
        |tableA.join(tableB).
        |  where(col("a.id")===col("b.id")).
        |  select(col("a.id").alias("a_id"), col("b.id").alias("b_id")).
        |  collect().
        |  mkString("[", ", ", "]")
        |""".stripMargin
    val output = runCommandsInShell(input)
    assertContains("""String = "[[1,1]]"""", output)
  }

  test("closure cleaner") {
    val input =
      """
        |class NonSerializable(val id: Int = -1) { }
        |
        |{
        |  val x = 100
        |  val y = new NonSerializable
        |}
        |
        |val t = 200
        |
        |{
        |  def foo(): Int = { x }
        |  def bar(): Int = { y.id }
        |  val z = new NonSerializable
        |}
        |
        |{
        |  val myLambda = (a: Int) => a + t + foo()
        |  val myUdf = udf(myLambda)
        |}
        |
        |spark.range(0, 10).
        |  withColumn("result", myUdf(col("id"))).
        |  agg(sum("result")).
        |  collect()(0)(0).asInstanceOf[Long]
        |""".stripMargin
    val output = runCommandsInShell(input)
    assertContains(": Long = 3045", output)
  }

  test("closure cleaner with function") {
    val input =
      """
        |class NonSerializable(val id: Int = -1) { }
        |
        |{
        |  val x = 100
        |  val y = new NonSerializable
        |}
        |
        |{
        |  def foo(): Int = { x }
        |  def bar(): Int = { y.id }
        |  val z = new NonSerializable
        |}
        |
        |def example() = {
        |  val myLambda = (a: Int) => a + foo()
        |  val myUdf = udf(myLambda)
        |  spark.range(0, 10).
        |    withColumn("result", myUdf(col("id"))).
        |    agg(sum("result")).
        |    collect()(0)(0).asInstanceOf[Long]
        |}
        |
        |example()
        |""".stripMargin
    val output = runCommandsInShell(input)
    assertContains(": Long = 1045", output)
  }

  test("closure cleaner nested") {
    val input =
      """
        |class NonSerializable(val id: Int = -1) { }
        |
        |{
        |  val x = 100
        |  val y = new NonSerializable
        |}
        |
        |{
        |  def foo(): Int = { x }
        |  def bar(): Int = { y.id }
        |  val z = new NonSerializable
        |}
        |
        |val example = () => {
        |  val nested = () => {
        |    val myLambda = (a: Int) => a + foo()
        |    val myUdf = udf(myLambda)
        |    spark.range(0, 10).
        |      withColumn("result", myUdf(col("id"))).
        |      agg(sum("result")).
        |      collect()(0)(0).asInstanceOf[Long]
        |  }
        |  nested()
        |}
        |example()
        |""".stripMargin
    val output = runCommandsInShell(input)
    assertContains(": Long = 1045", output)
  }

  test("closure cleaner with enclosing lambdas") {
    val input =
      """
        |class NonSerializable(val id: Int = -1) { }
        |
        |{
        |  val x = 100
        |  val y = new NonSerializable
        |}
        |
        |val z = new NonSerializable
        |
        |spark.range(0, 10).
        |// for this call UdfUtils will create a new lambda and this lambda becomes enclosing
        |  map(i => i + x).
        |  agg(sum("value")).
        |  collect()(0)(0).asInstanceOf[Long]
        |""".stripMargin
    val output = runCommandsInShell(input)
    assertContains(": Long = 1045", output)
  }

  test("closure cleaner cleans capturing class") {
    val input =
      """
        |class NonSerializable(val id: Int = -1) { }
        |
        |{
        |  val x = 100
        |  val y = new NonSerializable
        |}
        |
        |class Test extends Serializable {
        |  // capturing class is cmd$Helper$Test
        |  val myUdf = udf((i: Int) => i + x)
        |  val z = new NonSerializable
        |  val res = spark.range(0, 10).
        |    withColumn("result", myUdf(col("id"))).
        |    agg(sum("result")).
        |    collect()(0)(0).asInstanceOf[Long]
        |}
        |(new Test()).res
        |""".stripMargin
    val output = runCommandsInShell(input)
    assertContains(": Long = 1045", output)
  }

  test("Simple configuration set in startup") {
    val input =
      """
        |spark.conf.get("spark.sql.abc")
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("abc", output)
  }
}
