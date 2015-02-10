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
import scala.collection.mutable

import com.google.common.base.Charsets.UTF_8
import com.google.common.io.ByteStreams
import com.google.common.io.Files
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import org.apache.spark.{Logging, SparkConf, SparkContext, SparkException, TestUtils}
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded}
import org.apache.spark.util.Utils

/**
 * Integration tests for YARN; these tests use a mini Yarn cluster to run Spark-on-YARN
 * applications, and require the Spark assembly to be built before they can be successfully
 * run.
 */
class YarnClusterSuite extends FunSuite with BeforeAndAfterAll with Matchers with Logging {

  // log4j configuration for the YARN containers, so that their output is collected
  // by YARN instead of trying to overwrite unit-tests.log.
  private val LOG4J_CONF = """
    |log4j.rootCategory=DEBUG, console
    |log4j.appender.console=org.apache.log4j.ConsoleAppender
    |log4j.appender.console.target=System.err
    |log4j.appender.console.layout=org.apache.log4j.PatternLayout
    |log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
    """.stripMargin

  private val TEST_PYFILE = """
    |import sys
    |from operator import add
    |
    |from pyspark import SparkConf , SparkContext
    |if __name__ == "__main__":
    |    if len(sys.argv) != 2:
    |        print >> sys.stderr, "Usage: test.py [result file]"
    |        exit(-1)
    |    sc = SparkContext(conf=SparkConf())
    |    status = open(sys.argv[1],'w')
    |    result = "failure"
    |    rdd = sc.parallelize(range(10))
    |    cnt = rdd.count()
    |    if cnt == 10:
    |        result = "success"
    |    status.write(result)
    |    status.close()
    |    sc.stop()
    """.stripMargin

  private var yarnCluster: MiniYARNCluster = _
  private var tempDir: File = _
  private var fakeSparkJar: File = _
  private var logConfDir: File = _

  override def beforeAll() {
    super.beforeAll()

    tempDir = Utils.createTempDir()
    logConfDir = new File(tempDir, "log4j")
    logConfDir.mkdir()

    val logConfFile = new File(logConfDir, "log4j.properties")
    Files.write(LOG4J_CONF, logConfFile, UTF_8)

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

    fakeSparkJar = File.createTempFile("sparkJar", null, tempDir)
  }

  override def afterAll() {
    yarnCluster.stop()
    super.afterAll()
  }

  test("run Spark in yarn-client mode") {
    testBasicYarnApp(true)
  }

  test("run Spark in yarn-cluster mode") {
    testBasicYarnApp(false)
  }

  test("run Spark in yarn-cluster mode unsuccessfully") {
    // Don't provide arguments so the driver will fail.
    val exception = intercept[SparkException] {
      runSpark(false, mainClassName(YarnClusterDriver.getClass))
      fail("Spark application should have failed.")
    }
  }

  test("run Python application in yarn-cluster mode") {
    val primaryPyFile = new File(tempDir, "test.py")
    Files.write(TEST_PYFILE, primaryPyFile, UTF_8)
    val pyFile = new File(tempDir, "test2.py")
    Files.write(TEST_PYFILE, pyFile, UTF_8)
    var result = File.createTempFile("result", null, tempDir)

    // The sbt assembly does not include pyspark / py4j python dependencies, so we need to
    // propagate SPARK_HOME so that those are added to PYTHONPATH. See PythonUtils.scala.
    val sparkHome = sys.props("spark.test.home")
    val extraConf = Map(
      "spark.executorEnv.SPARK_HOME" -> sparkHome,
      "spark.yarn.appMasterEnv.SPARK_HOME" -> sparkHome)

    runSpark(false, primaryPyFile.getAbsolutePath(),
      sparkArgs = Seq("--py-files", pyFile.getAbsolutePath()),
      appArgs = Seq(result.getAbsolutePath()),
      extraConf = extraConf)
    checkResult(result)
  }

  test("user class path first in client mode") {
    testUseClassPathFirst(true)
  }

  test("user class path first in cluster mode") {
    testUseClassPathFirst(false)
  }

  private def testBasicYarnApp(clientMode: Boolean): Unit = {
    var result = File.createTempFile("result", null, tempDir)
    runSpark(clientMode, mainClassName(YarnClusterDriver.getClass),
      appArgs = Seq(result.getAbsolutePath()))
    checkResult(result)
  }

  private def testUseClassPathFirst(clientMode: Boolean): Unit = {
    // Create a jar file that contains a different version of "test.resource".
    val originalJar = TestUtils.createJarWithFiles(Map("test.resource" -> "ORIGINAL"), tempDir)
    val userJar = TestUtils.createJarWithFiles(Map("test.resource" -> "OVERRIDDEN"), tempDir)
    val driverResult = File.createTempFile("driver", null, tempDir)
    val executorResult = File.createTempFile("executor", null, tempDir)
    runSpark(clientMode, mainClassName(YarnClasspathTest.getClass),
      appArgs = Seq(driverResult.getAbsolutePath(), executorResult.getAbsolutePath()),
      extraClassPath = Seq(originalJar.getPath()),
      extraJars = Seq("local:" + userJar.getPath()),
      extraConf = Map(
        "spark.driver.userClassPathFirst" -> "true",
        "spark.executor.userClassPathFirst" -> "true"))
    checkResult(driverResult, "OVERRIDDEN")
    checkResult(executorResult, "OVERRIDDEN")
  }

  private def runSpark(
      clientMode: Boolean,
      klass: String,
      appArgs: Seq[String] = Nil,
      sparkArgs: Seq[String] = Nil,
      extraClassPath: Seq[String] = Nil,
      extraJars: Seq[String] = Nil,
      extraConf: Map[String, String] = Map()): Unit = {
    val master = if (clientMode) "yarn-client" else "yarn-cluster"
    val props = new Properties()

    props.setProperty("spark.yarn.jar", "local:" + fakeSparkJar.getAbsolutePath())

    val childClasspath = logConfDir.getAbsolutePath() +
      File.pathSeparator +
      sys.props("java.class.path") +
      File.pathSeparator +
      extraClassPath.mkString(File.pathSeparator)
    props.setProperty("spark.driver.extraClassPath", childClasspath)
    props.setProperty("spark.executor.extraClassPath", childClasspath)

    // SPARK-4267: make sure java options are propagated correctly.
    props.setProperty("spark.driver.extraJavaOptions", "-Dfoo=\"one two three\"")
    props.setProperty("spark.executor.extraJavaOptions", "-Dfoo=\"one two three\"")

    yarnCluster.getConfig().foreach { e =>
      props.setProperty("spark.hadoop." + e.getKey(), e.getValue())
    }

    sys.props.foreach { case (k, v) =>
      if (k.startsWith("spark.")) {
        props.setProperty(k, v)
      }
    }

    extraConf.foreach { case (k, v) => props.setProperty(k, v) }

    val propsFile = File.createTempFile("spark", ".properties", tempDir)
    val writer = new OutputStreamWriter(new FileOutputStream(propsFile), UTF_8)
    props.store(writer, "Spark properties.")
    writer.close()

    val extraJarArgs = if (!extraJars.isEmpty()) Seq("--jars", extraJars.mkString(",")) else Nil
    val mainArgs =
      if (klass.endsWith(".py")) {
        Seq(klass)
      } else {
        Seq("--class", klass, fakeSparkJar.getAbsolutePath())
      }
    val argv =
      Seq(
        new File(sys.props("spark.test.home"), "bin/spark-submit").getAbsolutePath(),
        "--master", master,
        "--num-executors", "1",
        "--properties-file", propsFile.getAbsolutePath()) ++
      extraJarArgs ++
      sparkArgs ++
      mainArgs ++
      appArgs

    Utils.executeAndGetOutput(argv,
      extraEnvironment = Map("YARN_CONF_DIR" -> tempDir.getAbsolutePath()))
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

  private def mainClassName(klass: Class[_]): String = {
    klass.getName().stripSuffix("$")
  }

}

private class SaveExecutorInfo extends SparkListener {
  val addedExecutorInfos = mutable.Map[String, ExecutorInfo]()

  override def onExecutorAdded(executor : SparkListenerExecutorAdded) {
    addedExecutorInfos(executor.executorId) = executor.executorInfo
  }
}

private object YarnClusterDriver extends Logging with Matchers {

  val WAIT_TIMEOUT_MILLIS = 10000
  var listener: SaveExecutorInfo = null

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println(
        s"""
        |Invalid command line: ${args.mkString(" ")}
        |
        |Usage: YarnClusterDriver [result file]
        """.stripMargin)
      System.exit(1)
    }

    listener = new SaveExecutorInfo
    val sc = new SparkContext(new SparkConf()
      .setAppName("yarn \"test app\" 'with quotes' and \\back\\slashes and $dollarSigns"))
    sc.addSparkListener(listener)
    val status = new File(args(0))
    var result = "failure"
    try {
      val data = sc.parallelize(1 to 4, 4).collect().toSet
      assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))
      data should be (Set(1, 2, 3, 4))
      result = "success"
    } finally {
      sc.stop()
      Files.write(result, status, UTF_8)
    }

    // verify log urls are present
    listener.addedExecutorInfos.values.foreach { info =>
      assert(info.logUrlMap.nonEmpty)
    }
  }

}

private object YarnClasspathTest {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println(
        s"""
        |Invalid command line: ${args.mkString(" ")}
        |
        |Usage: YarnClasspathTest [driver result file] [executor result file]
        """.stripMargin)
      System.exit(1)
    }

    readResource(args(0))
    val sc = new SparkContext(new SparkConf())
    try {
      sc.parallelize(Seq(1)).foreach { x => readResource(args(1)) }
    } finally {
      sc.stop()
    }
  }

  private def readResource(resultPath: String): Unit = {
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
