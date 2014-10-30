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

package org.apache.spark.deploy.yarn

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._

import com.google.common.base.Charsets.UTF_8
import com.google.common.io.ByteStreams
import com.google.common.io.Files
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster

import org.apache.spark.{Logging, SparkConf, SparkContext, SparkException, TestUtils}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.Utils

class YarnClusterSuite extends FunSuite with BeforeAndAfterAll with Matchers with Logging {

  // log4j configuration for the Yarn containers, so that their output is collected
  // by Yarn instead of trying to overwrite unit-tests.log.
  private val LOG4J_CONF = """
    |log4j.rootCategory=DEBUG, console
    |log4j.appender.console=org.apache.log4j.ConsoleAppender
    |log4j.appender.console.target=System.err
    |log4j.appender.console.layout=org.apache.log4j.PatternLayout
    |log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
    """.stripMargin

  private var yarnCluster: MiniYARNCluster = _
  private var tempDir: File = _
  private var fakeSparkJar: File = _
  private var oldConf: Map[String, String] = _

  override def beforeAll() {
    tempDir = Utils.createTempDir()

    val logConfDir = new File(tempDir, "log4j")
    logConfDir.mkdir()

    val logConfFile = new File(logConfDir, "log4j.properties")
    Files.write(LOG4J_CONF, logConfFile, UTF_8)

    val childClasspath = logConfDir.getAbsolutePath() + File.pathSeparator +
      sys.props("java.class.path")

    oldConf = sys.props.filter { case (k, v) => k.startsWith("spark.") }.toMap

    yarnCluster = new MiniYARNCluster(getClass().getName(), 1, 1, 1)
    yarnCluster.init(new YarnConfiguration())
    yarnCluster.start()

    // There's a race in MiniYARNCluster in which start() may return before the RM has updated
    // its address in the configuration. You can see this in the logs by noticing that when
    // MiniYARNCluster prints the address, it still has port "0" assigned, although later the
    // test works sometimes:
    //
    //    INFO MiniYARNCluster: MiniYARN ResourceManager address: blah:0
    //
    // That log message prints the contents of the RM_ADDRESS config variable. If you check it
    // later on, it looks something like this:
    //
    //    INFO YarnClusterSuite: RM address in configuration is blah:42631
    //
    // This hack loops for a bit waiting for the port to change, and fails the test if it hasn't
    // done so in a timely manner (defined to be 10 seconds).
    val config = yarnCluster.getConfig()
    val deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10)
    while (config.get(YarnConfiguration.RM_ADDRESS).split(":")(1) == "0") {
      if (System.currentTimeMillis() > deadline) {
        throw new IllegalStateException("Timed out waiting for RM to come up.")
      }
      logDebug("RM address still not set in configuration, waiting...")
      TimeUnit.MILLISECONDS.sleep(100)
    }

    logInfo(s"RM address in configuration is ${config.get(YarnConfiguration.RM_ADDRESS)}")
    config.foreach { e =>
      sys.props += ("spark.hadoop." + e.getKey() -> e.getValue())
    }

    fakeSparkJar = File.createTempFile("sparkJar", null, tempDir)
    sys.props += ("spark.yarn.jar" -> ("local:" + fakeSparkJar.getAbsolutePath()))
    sys.props += ("spark.executor.instances" -> "1")
    sys.props += ("spark.driver.extraClassPath" -> childClasspath)
    sys.props += ("spark.executor.extraClassPath" -> childClasspath)

    super.beforeAll()
  }

  override def afterAll() {
    yarnCluster.stop()
    sys.props.retain { case (k, v) => !k.startsWith("spark.") }
    sys.props ++= oldConf
    super.afterAll()
  }

  test("run Spark in yarn-client mode") {
    var result = File.createTempFile("result", null, tempDir)
    YarnClusterDriver.main(Array("yarn-client", result.getAbsolutePath()))
    checkResult(result)
  }

  test("run Spark in yarn-cluster mode") {
    val main = YarnClusterDriver.getClass.getName().stripSuffix("$")
    var result = File.createTempFile("result", null, tempDir)
    runSpark(false, YarnClusterDriver.getClass, Seq(result.getAbsolutePath()))
    checkResult(result)
  }

  test("run Spark in yarn-cluster mode unsuccessfully") {
    // Don't provide arguments so the driver will fail.
    val exception = intercept[SparkException] {
      runSpark(false, YarnClusterDriver.getClass)
    }
    assert(Utils.exceptionString(exception).contains("Application finished with failed status"))
  }

  test("user class path first in client mode") {
    testClassPathIsolation(true)
  }

  test("user class path first in cluster mode") {
    testClassPathIsolation(false)
  }

  private def testClassPathIsolation(clientMode: Boolean) = {
    // Create a jar file that contains a different version of "test.resource".
    val jarFile = TestUtils.createJarWithFiles(Map("test.resource" -> "OVERRIDDEN"), tempDir)
    val driverResult = File.createTempFile("driver", null, tempDir)
    val executorResult = File.createTempFile("executor", null, tempDir)
    runSpark(clientMode, YarnClasspathTest.getClass,
      Seq(driverResult.getAbsolutePath(), executorResult.getAbsolutePath()),
      Seq("local:" + jarFile.getPath()),
      Map(
        "spark.driver.userClassPathFirst" -> "true",
        "spark.executor.userClassPathFirst" -> "true"))
    checkResult(driverResult, "OVERRIDDEN")
    checkResult(executorResult, "OVERRIDDEN")
  }

  // Note: calling this method in client mode requires the Spark assembly to have been built,
  // since it uses spark-submit.
  private def runSpark(
      clientMode: Boolean,
      klass: Class[_],
      args: Seq[String] = Nil,
      extraJars: Seq[String] = Nil,
      settings: Map[String, String] = Map()) = {
    if (clientMode) {
      val main = klass.getName().stripSuffix("$")
      val userJars = extraJars.mkString(",")

      val props = new Properties()
      sys.props
        .filter { case (k, v) => k.startsWith("spark.") }
        .foreach { case (k, v) => props.setProperty(k, v) }
      settings.foreach { case (k, v) => props.setProperty(k, v) }

      val propsFile = File.createTempFile("client", ".properties", tempDir)
      val writer = new OutputStreamWriter(new FileOutputStream(propsFile), UTF_8)
      props.store(writer, "Client mode properties.")
      writer.close()

      val argv = Seq(
        new File(sys.props("spark.test.home"), "bin/spark-submit").getAbsolutePath(),
        "--master", "yarn-client",
        "--class", main,
        "--jars", extraJars.mkString(","),
        "--num-executors", "1",
        "--properties-file", propsFile.getAbsolutePath(),
        fakeSparkJar.getAbsolutePath()) ++
        Seq("yarn-client") ++
        args

      Utils.executeAndGetOutput(argv,
        extraEnvironment = Map("YARN_CONF_DIR" -> tempDir.getAbsolutePath()))
    } else {
      val main = klass.getName().stripSuffix("$")
      val userJars = if (!extraJars.isEmpty()) Seq("--addJars", extraJars.mkString(",")) else Nil

      val clientArgs = Array("--class", main,
        "--jar", "file:" + fakeSparkJar.getAbsolutePath(),
        "--arg", "yarn-cluster",
        "--num-executors", "1") ++
        userJars ++
        args.flatMap { Seq("--arg", _) }

      sys.props ++= settings
      try {
        Client.main(clientArgs)
      } finally {
        sys.props --= settings.keys
      }
    }
  }

  /**
   * This is a workaround for an issue with yarn-cluster mode: the Client class will not provide
   * any sort of error when the job process finishes successfully, but the job itself fails. So
   * the tests enforce that something is written to a file after everything is ok to indicate
   * that the job succeeded.
   */
  private def checkResult(result: File): Unit = {
    checkResult(result, "success")
  }

  private def checkResult(result: File, expected: String): Unit = {
    var resultString = Files.toString(result, UTF_8)
    resultString should be (expected)
  }

}

private object YarnClusterDriver extends Logging with Matchers {

  def main(args: Array[String]) = {
    if (args.length != 2) {
      System.err.println(
        s"""
        |Invalid command line: ${args.mkString(" ")}
        |
        |Usage: YarnClusterDriver [master] [result file]
        """.stripMargin)
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf().setMaster(args(0))
      .setAppName("yarn \"test app\" 'with quotes' and \\back\\slashes and $dollarSigns"))
    val status = new File(args(1))
    var result = "failure"
    try {
      val data = sc.parallelize(1 to 4, 4).collect().toSet
      data should be (Set(1, 2, 3, 4))
      result = "success"
    } finally {
      sc.stop()
      Files.write(result, status, UTF_8)
    }
  }

}

private object YarnClasspathTest {

  def main(args: Array[String]) = {
    if (args.length != 3) {
      System.err.println(
        s"""
        |Invalid command line: ${args.mkString(" ")}
        |
        |Usage: YarnClasspathTest [master] [driver result file] [executor result file]
        """.stripMargin)
      System.exit(1)
    }

    readResource(args(1))
    val sc = new SparkContext(new SparkConf().setMaster(args(0))
      .setAppName("yarn \"test app\" 'with quotes' and \\back\\slashes and $dollarSigns"))
    val status = new File(args(1))
    var result = "failure"
    try {
      sc.parallelize(Seq(1)).foreach { x => readResource(args(2)) }
    } finally {
      sc.stop()
    }
  }

  private def readResource(resultPath: String) = {
    var result = "failure"
    try {
      val ccl = Thread.currentThread().getContextClassLoader()
      val resource = ccl.getResourceAsStream("test.resource")
      val bytes = ByteStreams.toByteArray(resource)
      result = new String(bytes, 0, bytes.length, UTF_8)
    } finally {
      Files.write(result, new File(resultPath), UTF_8)
    }
  }

}
