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

import java.io.File
import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.{HashMap => JHashMap}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

import com.google.common.io.{ByteStreams, Files}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.launcher._
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationStart,
  SparkListenerExecutorAdded}
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.tags.ExtendedYarnTest
import org.apache.spark.util.Utils

/**
 * Integration tests for YARN; these tests use a mini Yarn cluster to run Spark-on-YARN
 * applications, and require the Spark assembly to be built before they can be successfully
 * run.
 */
@ExtendedYarnTest
class YarnClusterSuite extends BaseYarnClusterSuite {

  override def newYarnConfig(): YarnConfiguration = new YarnConfiguration()

  private val TEST_PYFILE = """
    |import mod1, mod2
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
    |    rdd = sc.parallelize(range(10)).map(lambda x: x * mod1.func() * mod2.func())
    |    cnt = rdd.count()
    |    if cnt == 10:
    |        result = "success"
    |    status.write(result)
    |    status.close()
    |    sc.stop()
    """.stripMargin

  private val TEST_PYMODULE = """
    |def func():
    |    return 42
    """.stripMargin

  test("run Spark in yarn-client mode") {
    testBasicYarnApp(true)
  }

  test("run Spark in yarn-cluster mode") {
    testBasicYarnApp(false)
  }

  test("run Spark in yarn-client mode with different configurations") {
    testBasicYarnApp(true,
      Map(
        "spark.driver.memory" -> "512m",
        "spark.executor.cores" -> "1",
        "spark.executor.memory" -> "512m",
        "spark.executor.instances" -> "2"
      ))
  }

  test("run Spark in yarn-cluster mode with different configurations") {
    testBasicYarnApp(true,
      Map(
        "spark.driver.memory" -> "512m",
        "spark.driver.cores" -> "1",
        "spark.executor.cores" -> "1",
        "spark.executor.memory" -> "512m",
        "spark.executor.instances" -> "2"
      ))
  }

  test("run Spark in yarn-client mode with additional jar") {
    testWithAddJar(true)
  }

  test("run Spark in yarn-cluster mode with additional jar") {
    testWithAddJar(false)
  }

  test("run Spark in yarn-cluster mode unsuccessfully") {
    // Don't provide arguments so the driver will fail.
    val finalState = runSpark(false, mainClassName(YarnClusterDriver.getClass))
    finalState should be (SparkAppHandle.State.FAILED)
  }

  test("run Python application in yarn-client mode") {
    testPySpark(true)
  }

  test("run Python application in yarn-cluster mode") {
    testPySpark(false)
  }

  test("user class path first in client mode") {
    testUseClassPathFirst(true)
  }

  test("user class path first in cluster mode") {
    testUseClassPathFirst(false)
  }

  test("monitor app using launcher library") {
    val env = new JHashMap[String, String]()
    env.put("YARN_CONF_DIR", hadoopConfDir.getAbsolutePath())

    val propsFile = createConfFile()
    val handle = new SparkLauncher(env)
      .setSparkHome(sys.props("spark.test.home"))
      .setConf("spark.ui.enabled", "false")
      .setPropertiesFile(propsFile)
      .setMaster("yarn")
      .setDeployMode("client")
      .setAppResource(SparkLauncher.NO_RESOURCE)
      .setMainClass(mainClassName(YarnLauncherTestApp.getClass))
      .startApplication()

    try {
      eventually(timeout(30 seconds), interval(100 millis)) {
        handle.getState() should be (SparkAppHandle.State.RUNNING)
      }

      handle.getAppId() should not be (null)
      handle.getAppId() should startWith ("application_")
      handle.stop()

      eventually(timeout(30 seconds), interval(100 millis)) {
        handle.getState() should be (SparkAppHandle.State.KILLED)
      }
    } finally {
      handle.kill()
    }
  }

  private def testBasicYarnApp(clientMode: Boolean, conf: Map[String, String] = Map()): Unit = {
    val result = File.createTempFile("result", null, tempDir)
    val finalState = runSpark(clientMode, mainClassName(YarnClusterDriver.getClass),
      appArgs = Seq(result.getAbsolutePath()),
      extraConf = conf)
    checkResult(finalState, result)
  }

  private def testWithAddJar(clientMode: Boolean): Unit = {
    val originalJar = TestUtils.createJarWithFiles(Map("test.resource" -> "ORIGINAL"), tempDir)
    val driverResult = File.createTempFile("driver", null, tempDir)
    val executorResult = File.createTempFile("executor", null, tempDir)
    val finalState = runSpark(clientMode, mainClassName(YarnClasspathTest.getClass),
      appArgs = Seq(driverResult.getAbsolutePath(), executorResult.getAbsolutePath()),
      extraClassPath = Seq(originalJar.getPath()),
      extraJars = Seq("local:" + originalJar.getPath()))
    checkResult(finalState, driverResult, "ORIGINAL")
    checkResult(finalState, executorResult, "ORIGINAL")
  }

  private def testPySpark(clientMode: Boolean): Unit = {
    val primaryPyFile = new File(tempDir, "test.py")
    Files.write(TEST_PYFILE, primaryPyFile, StandardCharsets.UTF_8)

    // When running tests, let's not assume the user has built the assembly module, which also
    // creates the pyspark archive. Instead, let's use PYSPARK_ARCHIVES_PATH to point at the
    // needed locations.
    val sparkHome = sys.props("spark.test.home")
    val pythonPath = Seq(
        s"$sparkHome/python/lib/py4j-0.10.1-src.zip",
        s"$sparkHome/python")
    val extraEnv = Map(
      "PYSPARK_ARCHIVES_PATH" -> pythonPath.map("local:" + _).mkString(File.pathSeparator),
      "PYTHONPATH" -> pythonPath.mkString(File.pathSeparator))

    val moduleDir =
      if (clientMode) {
        // In client-mode, .py files added with --py-files are not visible in the driver.
        // This is something that the launcher library would have to handle.
        tempDir
      } else {
        val subdir = new File(tempDir, "pyModules")
        subdir.mkdir()
        subdir
      }
    val pyModule = new File(moduleDir, "mod1.py")
    Files.write(TEST_PYMODULE, pyModule, StandardCharsets.UTF_8)

    val mod2Archive = TestUtils.createJarWithFiles(Map("mod2.py" -> TEST_PYMODULE), moduleDir)
    val pyFiles = Seq(pyModule.getAbsolutePath(), mod2Archive.getPath()).mkString(",")
    val result = File.createTempFile("result", null, tempDir)

    val finalState = runSpark(clientMode, primaryPyFile.getAbsolutePath(),
      sparkArgs = Seq("--py-files" -> pyFiles),
      appArgs = Seq(result.getAbsolutePath()),
      extraEnv = extraEnv)
    checkResult(finalState, result)
  }

  private def testUseClassPathFirst(clientMode: Boolean): Unit = {
    // Create a jar file that contains a different version of "test.resource".
    val originalJar = TestUtils.createJarWithFiles(Map("test.resource" -> "ORIGINAL"), tempDir)
    val userJar = TestUtils.createJarWithFiles(Map("test.resource" -> "OVERRIDDEN"), tempDir)
    val driverResult = File.createTempFile("driver", null, tempDir)
    val executorResult = File.createTempFile("executor", null, tempDir)
    val finalState = runSpark(clientMode, mainClassName(YarnClasspathTest.getClass),
      appArgs = Seq(driverResult.getAbsolutePath(), executorResult.getAbsolutePath()),
      extraClassPath = Seq(originalJar.getPath()),
      extraJars = Seq("local:" + userJar.getPath()),
      extraConf = Map(
        "spark.driver.userClassPathFirst" -> "true",
        "spark.executor.userClassPathFirst" -> "true"))
    checkResult(finalState, driverResult, "OVERRIDDEN")
    checkResult(finalState, executorResult, "OVERRIDDEN")
  }

}

private[spark] class SaveExecutorInfo extends SparkListener {
  val addedExecutorInfos = mutable.Map[String, ExecutorInfo]()
  var driverLogs: Option[collection.Map[String, String]] = None

  override def onExecutorAdded(executor: SparkListenerExecutorAdded) {
    addedExecutorInfos(executor.executorId) = executor.executorInfo
  }

  override def onApplicationStart(appStart: SparkListenerApplicationStart): Unit = {
    driverLogs = appStart.driverLogs
  }
}

private object YarnClusterDriver extends Logging with Matchers {

  val WAIT_TIMEOUT_MILLIS = 10000

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      // scalastyle:off println
      System.err.println(
        s"""
        |Invalid command line: ${args.mkString(" ")}
        |
        |Usage: YarnClusterDriver [result file]
        """.stripMargin)
      // scalastyle:on println
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf()
      .set("spark.extraListeners", classOf[SaveExecutorInfo].getName)
      .setAppName("yarn \"test app\" 'with quotes' and \\back\\slashes and $dollarSigns"))
    val conf = sc.getConf
    val status = new File(args(0))
    var result = "failure"
    try {
      val data = sc.parallelize(1 to 4, 4).collect().toSet
      sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
      data should be (Set(1, 2, 3, 4))
      result = "success"
    } finally {
      Files.write(result, status, StandardCharsets.UTF_8)
      sc.stop()
    }

    // verify log urls are present
    val listeners = sc.listenerBus.findListenersByClass[SaveExecutorInfo]
    assert(listeners.size === 1)
    val listener = listeners(0)
    val executorInfos = listener.addedExecutorInfos.values
    assert(executorInfos.nonEmpty)
    executorInfos.foreach { info =>
      assert(info.logUrlMap.nonEmpty)
    }

    // If we are running in yarn-cluster mode, verify that driver logs links and present and are
    // in the expected format.
    if (conf.get("spark.master") == "yarn-cluster") {
      assert(listener.driverLogs.nonEmpty)
      val driverLogs = listener.driverLogs.get
      assert(driverLogs.size === 2)
      assert(driverLogs.contains("stderr"))
      assert(driverLogs.contains("stdout"))
      val urlStr = driverLogs("stderr")
      // Ensure that this is a valid URL, else this will throw an exception
      new URL(urlStr)
      val containerId = YarnSparkHadoopUtil.get.getContainerId
      val user = Utils.getCurrentUserName()
      assert(urlStr.endsWith(s"/node/containerlogs/$containerId/$user/stderr?start=-4096"))
    }
  }

}

private object YarnClasspathTest extends Logging {

  var exitCode = 0

  def error(m: String, ex: Throwable = null): Unit = {
    logError(m, ex)
    // scalastyle:off println
    System.out.println(m)
    if (ex != null) {
      ex.printStackTrace(System.out)
    }
    // scalastyle:on println
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      error(
        s"""
        |Invalid command line: ${args.mkString(" ")}
        |
        |Usage: YarnClasspathTest [driver result file] [executor result file]
        """.stripMargin)
      // scalastyle:on println
    }

    readResource(args(0))
    val sc = new SparkContext(new SparkConf())
    try {
      sc.parallelize(Seq(1)).foreach { x => readResource(args(1)) }
    } finally {
      sc.stop()
    }
    System.exit(exitCode)
  }

  private def readResource(resultPath: String): Unit = {
    var result = "failure"
    try {
      val ccl = Thread.currentThread().getContextClassLoader()
      val resource = ccl.getResourceAsStream("test.resource")
      val bytes = ByteStreams.toByteArray(resource)
      result = new String(bytes, 0, bytes.length, StandardCharsets.UTF_8)
    } catch {
      case t: Throwable =>
        error(s"loading test.resource to $resultPath", t)
        // set the exit code if not yet set
        exitCode = 2
    } finally {
      Files.write(result, new File(resultPath), StandardCharsets.UTF_8)
    }
  }

}

private object YarnLauncherTestApp {

  def main(args: Array[String]): Unit = {
    // Do not stop the application; the test will stop it using the launcher lib. Just run a task
    // that will prevent the process from exiting.
    val sc = new SparkContext(new SparkConf())
    sc.parallelize(Seq(1)).foreach { i =>
      this.synchronized {
        wait()
      }
    }
  }

}
