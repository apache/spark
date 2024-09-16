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
import java.nio.file.Files

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{Logger, LoggerContext}

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

class ReplSuite extends SparkFunSuite {

  private var originalClassLoader: ClassLoader = null

  override def beforeAll(): Unit = {
    originalClassLoader = Thread.currentThread().getContextClassLoader
  }

  override def afterAll(): Unit = {
    if (originalClassLoader != null) {
      // Reset the class loader to not affect other suites. REPL will set its own class loader but
      // doesn't reset it.
      Thread.currentThread().setContextClassLoader(originalClassLoader)
    }
  }

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

  test("SPARK-15236: use Hive catalog") {
    // turn on the INFO log so that it is possible the code will dump INFO
    // entry for using "HiveMetastore"
    val rootLogger = LogManager.getRootLogger().asInstanceOf[Logger]
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
    val rootLogger = LogManager.getRootLogger().asInstanceOf[Logger]
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

  test("SPARK-30167: Log4j configuration for REPL should override root logger properly") {
    val testConfiguration =
      """
        |# Set everything to be logged to the console
        |rootLogger.level = info
        |rootLogger.appenderRef.stdout.ref = console
        |
        |appender.console.type = Console
        |appender.console.name = console
        |appender.console.target = SYSTEM_ERR
        |appender.console.follow = true
        |appender.console.layout.type = PatternLayout
        |appender.console.layout.pattern = %d{HH:mm:ss.SSS} %p %c: %maxLen{%m}{512}%n%ex{8}%n
        |
        |# Set the log level for this class to ERROR same as the default setting.
        |logger.repl.name = org.apache.spark.repl.Main
        |logger.repl.level = error
        |
        |logger.customLogger2.name = customLogger2
        |logger.customLogger2.level = info
        |""".stripMargin

    val log4jprops = Files.createTempFile("log4j2.properties.d", "log4j2.properties")
    Files.write(log4jprops, testConfiguration.getBytes)

    val originalRootLogger = LogManager.getRootLogger.asInstanceOf[Logger]
    val originalRootAppender = originalRootLogger.getAppenders.get("file")
    val originalStderr = System.err
    val originalReplThresholdLevel = Logging.sparkShellThresholdLevel

    val replLoggerLogMessage = "Log level for REPL: "
    val warnLogMessage1 = "warnLogMessage1 should not be output"
    val errorLogMessage1 = "errorLogMessage1 should be output"
    val infoLogMessage1 = "infoLogMessage2 should be output"
    val infoLogMessage2 = "infoLogMessage3 should be output"
    val debugLogMessage1 = "debugLogMessage1 should be output"

    val out = try {
      val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
      context.setConfigLocation(log4jprops.toUri())

      // Re-initialization is needed to set SparkShellLoggingFilter to ConsoleAppender
      Main.initializeForcefully(true, false)
      // scalastyle:off
      runInterpreter("local",
        s"""
           |import java.io.{ByteArrayOutputStream, PrintStream}
           |
           |import org.apache.logging.log4j.{Level, LogManager}
           |import org.apache.logging.log4j.core.Logger
           |
           |val replLogger = LogManager.getLogger("${Main.getClass.getName.stripSuffix("$")}")
           |
           |// Log level for REPL is expected to be ERROR
           |"$replLoggerLogMessage" + replLogger.getLevel()
           |
           |val bout = new ByteArrayOutputStream()
           |
           |// Configure stderr to let log messages output to ByteArrayOutputStream.
           |val defaultErrStream: PrintStream = System.err
           |try {
           |  System.setErr(new PrintStream(bout))
           |
           |  // customLogger1 is not explicitly configured neither its log level nor appender
           |  // so this inherits the settings of rootLogger
           |  // but ConsoleAppender can use a different log level.
           |  val customLogger1 = LogManager.getLogger("customLogger1")
           |  customLogger1.warn("$warnLogMessage1")
           |  customLogger1.error("$errorLogMessage1")
           |
           |  // customLogger2 is explicitly configured its log level as INFO
           |  // so info level messages logged via customLogger2 should be output.
           |  val customLogger2 = LogManager.getLogger("customLogger2").asInstanceOf[Logger]
           |  customLogger2.info("$infoLogMessage1")
           |
           |  // customLogger2 is explicitly configured its log level
           |  // so its child should inherit the settings.
           |  val customLogger3 = LogManager.getLogger("customLogger2.child")
           |  customLogger3.info("$infoLogMessage2")
           |
           |  // customLogger4 is programmingly configured its log level as DEBUG
           |  // so debug level messages logged via customLogger4 should be output.
           |  val customLogger4 = LogManager.getLogger("customLogger4").asInstanceOf[Logger]
           |  customLogger4.setLevel(Level.DEBUG)
           |  customLogger4.debug("$debugLogMessage1")
           |
           |  // echo log messages
           |  bout.toString
           |} finally {
           |  System.setErr(defaultErrStream)
           |}
           |""".stripMargin)
    } finally {
      // Restore log4j settings for this suite
      val log4jproperties = Thread.currentThread()
        .getContextClassLoader.getResource("log4j2.properties")
      val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
      context.reconfigure()
      context.setConfigLocation(log4jproperties.toURI)
      context.updateLoggers()
      Logging.sparkShellThresholdLevel = originalReplThresholdLevel
    }
    // scalastyle:on

    // Ensure stderr configuration is successfully restored.
    assert(originalStderr eq System.err)

    // Ensure log4j settings are successfully restored.
    val restoredRootLogger = LogManager.getRootLogger.asInstanceOf[Logger]
    val restoredRootAppender = restoredRootLogger.getAppenders.get("file")
    assert(originalRootAppender.getClass == restoredRootAppender.getClass)
    assert(originalRootLogger.getLevel == restoredRootLogger.getLevel)

    // Ensure loggers added in this test case are successfully removed.
    assert(LogManager.getLogger("customLogger2").getLevel == Level.INFO)
    assert(LogManager.getLogger("customLogger2.child").getLevel == Level.INFO)

    // Ensure log level threshold for REPL is ERROR.
    assertContains(replLoggerLogMessage + "ERROR", out)

    assertDoesNotContain(warnLogMessage1, out)
    assertContains(errorLogMessage1, out)
    assertContains(infoLogMessage1, out)
    assertContains(infoLogMessage2, out)
    assertContains(debugLogMessage1, out)
  }

  test("propagation of local properties") {
    // A mock ILoop that doesn't install the SIGINT handler.
    class ILoop(out: PrintWriter) extends SparkILoop(null, out)

    val out = new StringWriter()
    Main.interp = new ILoop(new PrintWriter(out))
    Main.sparkContext = new SparkContext("local", "repl-test")
    val settings = new scala.tools.nsc.Settings
    settings.usejavacp.value = true
    Main.interp.createInterpreter(settings)

    Main.sparkContext.setLocalProperty("someKey", "someValue")

    // Make sure the value we set in the caller to interpret is propagated in the thread that
    // interprets the command.
    Main.interp.interpret("org.apache.spark.repl.Main.sparkContext.getLocalProperty(\"someKey\")")
    assert(out.toString.contains("someValue"))

    Main.sparkContext.stop()
    System.clearProperty("spark.driver.port")
  }

  test("register artifacts via SparkSession.addArtifact") {
  val artifactPath = new File("src/test/resources").toPath
    val intSumUdfPath = artifactPath.resolve("IntSumUdf.class")
    val output = runInterpreterInPasteMode("local",
      s"""
        |import org.apache.spark.sql.api.java.UDF2
        |import org.apache.spark.sql.types.DataTypes
        |
        |spark.addArtifact("${intSumUdfPath.toString}")
        |
        |spark.udf.registerJava("intSum", "IntSumUdf", DataTypes.LongType)
        |
        |val r = spark.range(5)
        |  .withColumn("id2", col("id") + 1)
        |  .selectExpr("intSum(id, id2)")
        |  .collect()
        |assert(r.map(_.getLong(0)).toSeq == Seq(1, 3, 5, 7, 9))
        |
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertDoesNotContain("assertion failed", output)
  }
}
