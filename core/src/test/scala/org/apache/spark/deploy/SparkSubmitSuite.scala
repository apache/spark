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

package org.apache.spark.deploy

import java.io._
import java.net.{URI, URL}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.collection.mutable.ArrayBuffer
import scala.io.{Codec, Source}

import com.google.common.io.ByteStreams
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FSDataInputStream, Path}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark._
import org.apache.spark.TestUtils
import org.apache.spark.TestUtils.JavaSourceFromString
import org.apache.spark.api.r.RUtils
import org.apache.spark.deploy.SparkSubmit._
import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.UI._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util.{CommandLineUtils, DependencyUtils, IvyTestUtils, ResetSystemProperties, Utils}
import org.apache.spark.util.MavenUtils.MavenCoordinate

trait TestPrematureExit {
  suite: SparkFunSuite =>

  private val noOpOutputStream = new OutputStream {
    def write(b: Int) = {}
  }

  /** Simple PrintStream that reads data into a buffer */
  private class BufferPrintStream extends PrintStream(noOpOutputStream) {
    val lineBuffer = ArrayBuffer[String]()
    // scalastyle:off println
    override def println(line: String): Unit = {
      lineBuffer += line
    }
    // scalastyle:on println
  }

  /** Returns true if the script exits and the given search string is printed. */
  private[spark] def testPrematureExit(
      input: Array[String],
      searchString: String,
      mainObject: CommandLineUtils = SparkSubmit) : Unit = {
    val printStream = new BufferPrintStream()
    mainObject.printStream = printStream

    @volatile var exitedCleanly = false
    val original = mainObject.exitFn
    mainObject.exitFn = (_) => exitedCleanly = true
    try {
      @volatile var exception: Exception = null
      val thread = new Thread {
        override def run() = try {
          mainObject.main(input)
        } catch {
          // Capture the exception to check whether the exception contains searchString or not
          case e: Exception => exception = e
        }
      }
      thread.start()
      thread.join()
      if (exitedCleanly) {
        val joined = printStream.lineBuffer.mkString("\n")
        assert(joined.contains(searchString))
      } else {
        assert(exception != null)
        if (!exception.getMessage.contains(searchString)) {
          throw exception
        }
      }
    } finally {
      mainObject.exitFn = original
    }
  }
}

// Note: this suite mixes in ResetSystemProperties because SparkSubmit.main() sets a bunch
// of properties that needed to be cleared after tests.
class SparkSubmitSuite
  extends SparkSubmitTestUtils
  with Matchers
  with BeforeAndAfterEach
  with ResetSystemProperties
  with TestPrematureExit {

  private val emptyIvySettings = File.createTempFile("ivy", ".xml")
  FileUtils.write(emptyIvySettings, "<ivysettings />", StandardCharsets.UTF_8)

  private val submit = new SparkSubmit()

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  // scalastyle:off println
  test("prints usage on empty input") {
    testPrematureExit(Array.empty[String], "Usage: spark-submit")
  }

  test("prints usage with only --help") {
    testPrematureExit(Array("--help"), "Usage: spark-submit")
  }

  test("prints error with unrecognized options") {
    testPrematureExit(Array("--blarg"), "Unrecognized option '--blarg'")
    testPrematureExit(Array("-bleg"), "Unrecognized option '-bleg'")
  }

  test("handle binary specified but not class") {
    val jar = TestUtils.createJarWithClasses(Seq("SparkSubmitClassA"))
    testPrematureExit(Array(jar.toString()), "No main class")
  }

  test("handles arguments with --key=val") {
    val clArgs = Seq(
      "--jars=one.jar,two.jar,three.jar",
      "--name=myApp",
      "--class=org.FooBar",
      SparkLauncher.NO_RESOURCE)
    val appArgs = new SparkSubmitArguments(clArgs)
    appArgs.jars should include regex (".*one.jar,.*two.jar,.*three.jar")
    appArgs.name should be ("myApp")
  }

  test("handles arguments to user program") {
    val clArgs = Seq(
      "--name", "myApp",
      "--class", "Foo",
      "userjar.jar",
      "some",
      "--weird", "args")
    val appArgs = new SparkSubmitArguments(clArgs)
    appArgs.childArgs should be (Seq("some", "--weird", "args"))
  }

  test("handles arguments to user program with name collision") {
    val clArgs = Seq(
      "--name", "myApp",
      "--class", "Foo",
      "userjar.jar",
      "--master", "local",
      "some",
      "--weird", "args")
    val appArgs = new SparkSubmitArguments(clArgs)
    appArgs.childArgs should be (Seq("--master", "local", "some", "--weird", "args"))
  }

  test("print the right queue name") {
    val clArgs = Seq(
      "--name", "myApp",
      "--class", "Foo",
      "--conf", "spark.yarn.queue=thequeue",
      "userjar.jar")
    val appArgs = new SparkSubmitArguments(clArgs)
    appArgs.queue should be ("thequeue")
    appArgs.toString should include ("thequeue")
  }

  test("SPARK-24241: do not fail fast if executor num is 0 when dynamic allocation is enabled") {
    val clArgs1 = Seq(
      "--name", "myApp",
      "--class", "Foo",
      "--num-executors", "0",
      "--conf", s"${DYN_ALLOCATION_ENABLED.key}=true",
      "thejar.jar")
    new SparkSubmitArguments(clArgs1)

    val clArgs2 = Seq(
      "--name", "myApp",
      "--class", "Foo",
      "--num-executors", "0",
      "--conf", s"${DYN_ALLOCATION_ENABLED.key}=false",
      "thejar.jar")

    val e = intercept[SparkException](new SparkSubmitArguments(clArgs2))
    assert(e.getMessage.contains("Number of executors must be a positive number"))
  }

  test("specify deploy mode through configuration") {
    val clArgs = Seq(
      "--master", "yarn",
      "--conf", "spark.submit.deployMode=client",
      "--class", "org.SomeClass",
      "thejar.jar"
    )
    val appArgs = new SparkSubmitArguments(clArgs)
    val (_, _, conf, _) = submit.prepareSubmitEnvironment(appArgs)

    appArgs.deployMode should be ("client")
    conf.get(SUBMIT_DEPLOY_MODE) should be ("client")

    // Both cmd line and configuration are specified, cmdline option takes the priority
    val clArgs1 = Seq(
      "--master", "yarn",
      "--deploy-mode", "cluster",
      "--conf", "spark.submit.deployMode=client",
      "--class", "org.SomeClass",
      "thejar.jar"
    )
    val appArgs1 = new SparkSubmitArguments(clArgs1)
    val (_, _, conf1, _) = submit.prepareSubmitEnvironment(appArgs1)

    appArgs1.deployMode should be ("cluster")
    conf1.get(SUBMIT_DEPLOY_MODE) should be ("cluster")

    // Neither cmdline nor configuration are specified, client mode is the default choice
    val clArgs2 = Seq(
      "--master", "yarn",
      "--class", "org.SomeClass",
      "thejar.jar"
    )
    val appArgs2 = new SparkSubmitArguments(clArgs2)
    appArgs2.deployMode should be (null)

    val (_, _, conf2, _) = submit.prepareSubmitEnvironment(appArgs2)
    appArgs2.deployMode should be ("client")
    conf2.get(SUBMIT_DEPLOY_MODE) should be ("client")
  }

  test("handles YARN cluster mode") {
    val clArgs = Seq(
      "--deploy-mode", "cluster",
      "--master", "yarn",
      "--executor-memory", "5g",
      "--executor-cores", "5",
      "--class", "org.SomeClass",
      "--jars", "one.jar,two.jar,three.jar",
      "--driver-memory", "4g",
      "--queue", "thequeue",
      "--files", "file1.txt,file2.txt",
      "--archives", "archive1.txt,archive2.txt",
      "--num-executors", "6",
      "--name", "beauty",
      "--conf", "spark.ui.enabled=false",
      "thejar.jar",
      "arg1", "arg2")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (childArgs, classpath, conf, mainClass) = submit.prepareSubmitEnvironment(appArgs)
    val childArgsStr = childArgs.mkString(" ")
    childArgsStr should include ("--class org.SomeClass")
    childArgsStr should include ("--arg arg1 --arg arg2")
    childArgsStr should include regex ("--jar .*thejar.jar")
    mainClass should be (SparkSubmit.YARN_CLUSTER_SUBMIT_CLASS)

    // In yarn cluster mode, also adding jars to classpath
    classpath(0) should endWith ("thejar.jar")
    classpath(1) should endWith ("one.jar")
    classpath(2) should endWith ("two.jar")
    classpath(3) should endWith ("three.jar")

    conf.get("spark.executor.memory") should be ("5g")
    conf.get("spark.driver.memory") should be ("4g")
    conf.get("spark.executor.cores") should be ("5")
    conf.get("spark.yarn.queue") should be ("thequeue")
    conf.get("spark.yarn.dist.jars") should include regex (".*one.jar,.*two.jar,.*three.jar")
    conf.get("spark.yarn.dist.files") should include regex (".*file1.txt,.*file2.txt")
    conf.get("spark.yarn.dist.archives") should include regex (".*archive1.txt,.*archive2.txt")
    conf.get("spark.app.name") should be ("beauty")
    conf.get(UI_ENABLED) should be (false)
    sys.props("SPARK_SUBMIT") should be ("true")
  }

  test("handles YARN client mode") {
    val clArgs = Seq(
      "--deploy-mode", "client",
      "--master", "yarn",
      "--executor-memory", "5g",
      "--executor-cores", "5",
      "--class", "org.SomeClass",
      "--jars", "one.jar,two.jar,three.jar",
      "--driver-memory", "4g",
      "--queue", "thequeue",
      "--files", "file1.txt,file2.txt",
      "--archives", "archive1.txt,archive2.txt",
      "--num-executors", "6",
      "--name", "trill",
      "--conf", "spark.ui.enabled=false",
      "thejar.jar",
      "arg1", "arg2")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (childArgs, classpath, conf, mainClass) = submit.prepareSubmitEnvironment(appArgs)
    childArgs.mkString(" ") should be ("arg1 arg2")
    mainClass should be ("org.SomeClass")
    classpath should have length (4)
    classpath(0) should endWith ("thejar.jar")
    classpath(1) should endWith ("one.jar")
    classpath(2) should endWith ("two.jar")
    classpath(3) should endWith ("three.jar")
    conf.get("spark.app.name") should be ("trill")
    conf.get("spark.executor.memory") should be ("5g")
    conf.get("spark.executor.cores") should be ("5")
    conf.get("spark.yarn.queue") should be ("thequeue")
    conf.get("spark.executor.instances") should be ("6")
    conf.get("spark.yarn.dist.files") should include regex (".*file1.txt,.*file2.txt")
    conf.get("spark.yarn.dist.archives") should include regex (".*archive1.txt,.*archive2.txt")
    conf.get("spark.yarn.dist.jars") should include
      regex (".*one.jar,.*two.jar,.*three.jar,.*thejar.jar")
    conf.get(UI_ENABLED) should be (false)
    sys.props("SPARK_SUBMIT") should be ("true")
  }

  test("SPARK-33530: handles standalone mode with archives") {
    val clArgs = Seq(
      "--master", "spark://localhost:1234",
      "--executor-memory", "5g",
      "--executor-cores", "5",
      "--class", "org.SomeClass",
      "--jars", "one.jar,two.jar,three.jar",
      "--driver-memory", "4g",
      "--files", "file1.txt,file2.txt",
      "--archives", "archive1.zip,archive2.jar",
      "--num-executors", "6",
      "--name", "beauty",
      "--conf", "spark.ui.enabled=false",
      "thejar.jar",
      "arg1", "arg2")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (childArgs, classpath, conf, mainClass) = submit.prepareSubmitEnvironment(appArgs)
    val childArgsStr = childArgs.mkString(" ")
    childArgsStr should include ("arg1 arg2")
    mainClass should be ("org.SomeClass")

    classpath(0) should endWith ("thejar.jar")
    classpath(1) should endWith ("one.jar")
    classpath(2) should endWith ("two.jar")
    classpath(3) should endWith ("three.jar")

    conf.get("spark.executor.memory") should be ("5g")
    conf.get("spark.driver.memory") should be ("4g")
    conf.get("spark.executor.cores") should be ("5")
    conf.get("spark.jars") should include regex (".*one.jar,.*two.jar,.*three.jar")
    conf.get("spark.files") should include regex (".*file1.txt,.*file2.txt")
    conf.get("spark.archives") should include regex (".*archive1.zip,.*archive2.jar")
    conf.get("spark.app.name") should be ("beauty")
    conf.get(UI_ENABLED) should be (false)
    sys.props("SPARK_SUBMIT") should be ("true")
  }

  test("handles standalone cluster mode") {
    testStandaloneCluster(useRest = true)
  }

  test("handles legacy standalone cluster mode") {
    testStandaloneCluster(useRest = false)
  }

  /**
   * Test whether the launch environment is correctly set up in standalone cluster mode.
   * @param useRest whether to use the REST submission gateway introduced in Spark 1.3
   */
  private def testStandaloneCluster(useRest: Boolean): Unit = {
    val clArgs = Seq(
      "--deploy-mode", "cluster",
      "--master", "spark://h:p",
      "--class", "org.SomeClass",
      "--supervise",
      "--driver-memory", "4g",
      "--driver-cores", "5",
      "--conf", "spark.ui.enabled=false",
      "thejar.jar",
      "arg1", "arg2")
    val appArgs = new SparkSubmitArguments(clArgs)
    appArgs.useRest = useRest
    val (childArgs, classpath, conf, mainClass) = submit.prepareSubmitEnvironment(appArgs)
    val childArgsStr = childArgs.mkString(" ")
    if (useRest) {
      childArgsStr should endWith ("thejar.jar org.SomeClass arg1 arg2")
      mainClass should be (SparkSubmit.REST_CLUSTER_SUBMIT_CLASS)
    } else {
      childArgsStr should startWith ("--supervise --memory 4g --cores 5")
      childArgsStr should include regex "launch spark://h:p .*thejar.jar org.SomeClass arg1 arg2"
      mainClass should be (SparkSubmit.STANDALONE_CLUSTER_SUBMIT_CLASS)
    }
    classpath should have size 0
    sys.props("SPARK_SUBMIT") should be ("true")

    val confMap = conf.getAll.toMap
    confMap.keys should contain ("spark.master")
    confMap.keys should contain ("spark.app.name")
    confMap.keys should contain (JARS.key)
    confMap.keys should contain ("spark.driver.memory")
    confMap.keys should contain (DRIVER_CORES.key)
    confMap.keys should contain (DRIVER_SUPERVISE.key)
    confMap.keys should contain (UI_ENABLED.key)
    confMap.keys should contain (SUBMIT_DEPLOY_MODE.key)
    conf.get(UI_ENABLED) should be (false)
  }

  test("handles standalone client mode") {
    val clArgs = Seq(
      "--deploy-mode", "client",
      "--master", "spark://h:p",
      "--executor-memory", "5g",
      "--total-executor-cores", "5",
      "--class", "org.SomeClass",
      "--driver-memory", "4g",
      "--conf", "spark.ui.enabled=false",
      "thejar.jar",
      "arg1", "arg2")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (childArgs, classpath, conf, mainClass) = submit.prepareSubmitEnvironment(appArgs)
    childArgs.mkString(" ") should be ("arg1 arg2")
    mainClass should be ("org.SomeClass")
    classpath should have length (1)
    classpath(0) should endWith ("thejar.jar")
    conf.get("spark.executor.memory") should be ("5g")
    conf.get("spark.cores.max") should be ("5")
    conf.get(UI_ENABLED) should be (false)
  }

  test("handles k8s cluster mode") {
    val clArgs = Seq(
      "--deploy-mode", "cluster",
      "--proxy-user", "test.user",
      "--master", "k8s://host:port",
      "--executor-memory", "5g",
      "--class", "org.SomeClass",
      "--driver-memory", "4g",
      "--conf", "spark.kubernetes.namespace=spark",
      "--conf", "spark.kubernetes.driver.container.image=bar",
      "/home/thejar.jar",
      "arg1")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (childArgs, classpath, conf, mainClass) = submit.prepareSubmitEnvironment(appArgs)

    val childArgsMap = childArgs.grouped(2).map(a => a(0) -> a(1)).toMap
    childArgsMap.get("--primary-java-resource").get should endWith ("/home/thejar.jar")
    childArgsMap.get("--main-class") should be (Some("org.SomeClass"))
    childArgsMap.get("--arg") should be (Some("arg1"))
    childArgsMap.get("--proxy-user") should be (Some("test.user"))
    mainClass should be (KUBERNETES_CLUSTER_SUBMIT_CLASS)
    classpath should have length (0)
    conf.get("spark.master") should be ("k8s://https://host:port")
    conf.get("spark.executor.memory") should be ("5g")
    conf.get("spark.driver.memory") should be ("4g")
    conf.get("spark.kubernetes.namespace") should be ("spark")
    conf.get("spark.kubernetes.driver.container.image") should be ("bar")
  }

  test("SPARK-35084: include jars of the --packages" +
    " in k8s client mode & driver runs inside a POD") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val main = MavenCoordinate("my.great.lib", "mylib", "0.1")
    val dep = MavenCoordinate("my.great.dep", "mylib", "0.1")
    IvyTestUtils.withRepository(main, Some(dep.toString), None) { repo =>
      val clArgs = Seq(
        "--class", JarCreationTest.getClass.getName.stripSuffix("$"),
        "--name", "testApp",
        "--deploy-mode", "client",
        "--proxy-user", "test.user",
        "--class", "org.SomeClass",
        "--master", "k8s://host:port",
        "--packages", Seq(main, dep).mkString(","),
        "--repositories", repo,
        "--conf", "spark.ui.enabled=false",
        "--conf", "spark.master.rest.enabled=false",
        "--conf", "spark.kubernetes.namespace=spark",
        "--conf", "spark.kubernetes.submitInDriver=true",
        "--conf", s"spark.jars.ivySettings=${emptyIvySettings.getAbsolutePath()}",
        unusedJar.toString,
        "my.great.lib.MyLib", "my.great.dep.MyLib")

      val appArgs = new SparkSubmitArguments(clArgs)
      try {
        val (_, _, sparkConf, _) = submit.prepareSubmitEnvironment(appArgs)
        sparkConf.get("spark.jars").contains("mylib") shouldBe true
      } finally {
        val mainJarPath = Paths.get("my.great.dep_mylib-0.1.jar")
        val depJarPath = Paths.get("my.great.lib_mylib-0.1.jar")
        if (Files.exists(mainJarPath)) {
          Files.delete(mainJarPath)
        }
        if (Files.exists(depJarPath)) {
          Files.delete(depJarPath)
        }
      }
    }
  }

  test("SPARK-47495: Not to add primary resource to jars again" +
    " in k8s client mode & driver runs inside a POD") {
    val clArgs = Seq(
      "--deploy-mode", "client",
      "--proxy-user", "test.user",
      "--master", "k8s://host:port",
      "--executor-memory", "1g",
      "--class", "org.SomeClass",
      "--driver-memory", "1g",
      "--conf", "spark.kubernetes.submitInDriver=true",
      "--jars", "src/test/resources/TestUDTF.jar",
      "/home/jarToIgnore.jar",
      "arg1")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (_, _, sparkConf, _) = submit.prepareSubmitEnvironment(appArgs)
    sparkConf.get("spark.jars").contains("jarToIgnore") shouldBe false
    sparkConf.get("spark.jars").contains("TestUDTF") shouldBe true
  }

  test("SPARK-33782: handles k8s files download to current directory") {
    val clArgs = Seq(
      "--deploy-mode", "client",
      "--proxy-user", "test.user",
      "--master", "k8s://host:port",
      "--executor-memory", "5g",
      "--class", "org.SomeClass",
      "--driver-memory", "4g",
      "--conf", "spark.kubernetes.namespace=spark",
      "--conf", "spark.kubernetes.driver.container.image=bar",
      "--conf", "spark.kubernetes.submitInDriver=true",
      "--files", "src/test/resources/test_metrics_config.properties",
      "--py-files", "src/test/resources/test_metrics_system.properties",
      "--archives", "src/test/resources/log4j2.properties",
      "--jars", "src/test/resources/TestUDTF.jar",
      "/home/thejar.jar",
      "arg1")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (childArgs, classpath, conf, mainClass) = submit.prepareSubmitEnvironment(appArgs)
    conf.get("spark.master") should be ("k8s://https://host:port")
    conf.get("spark.executor.memory") should be ("5g")
    conf.get("spark.driver.memory") should be ("4g")
    conf.get("spark.kubernetes.namespace") should be ("spark")
    conf.get("spark.kubernetes.driver.container.image") should be ("bar")

    Files.exists(Paths.get("test_metrics_config.properties")) should be (true)
    Files.exists(Paths.get("test_metrics_system.properties")) should be (true)
    Files.exists(Paths.get("log4j2.properties")) should be (true)
    Files.exists(Paths.get("TestUDTF.jar")) should be (true)
    Files.delete(Paths.get("test_metrics_config.properties"))
    Files.delete(Paths.get("test_metrics_system.properties"))
    Files.delete(Paths.get("log4j2.properties"))
    Files.delete(Paths.get("TestUDTF.jar"))
  }

  test("SPARK-47475: Avoid jars download if scheme matches " +
    "spark.kubernetes.jars.avoidDownloadSchemes " +
    "in k8s client mode & driver runs inside a POD") {
    val hadoopConf = new Configuration()
    updateConfWithFakeS3Fs(hadoopConf)
    withTempDir { tmpDir =>
      val notToDownload = File.createTempFile("NotToDownload", ".jar", tmpDir)
      val remoteJarFile = s"s3a://${notToDownload.getAbsolutePath}"

      val clArgs = Seq(
        "--deploy-mode", "client",
        "--proxy-user", "test.user",
        "--master", "k8s://host:port",
        "--class", "org.SomeClass",
        "--conf", "spark.kubernetes.submitInDriver=true",
        "--conf", "spark.kubernetes.jars.avoidDownloadSchemes=s3a",
        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.spark.deploy.TestFileSystem",
        "--conf", "spark.hadoop.fs.s3a.impl.disable.cache=true",
        "--files", "src/test/resources/test_metrics_config.properties",
        "--py-files", "src/test/resources/test_metrics_system.properties",
        "--archives", "src/test/resources/log4j2.properties",
        "--jars", s"src/test/resources/TestUDTF.jar,$remoteJarFile",
        "/home/jarToIgnore.jar",
        "arg1")
      val appArgs = new SparkSubmitArguments(clArgs)
      val (_, _, conf, _) = submit.prepareSubmitEnvironment(appArgs, Some(hadoopConf))
      conf.get("spark.master") should be("k8s://https://host:port")
      conf.get("spark.jars").contains(remoteJarFile) shouldBe true
      conf.get("spark.jars").contains("TestUDTF") shouldBe true

      Files.exists(Paths.get("test_metrics_config.properties")) should be(true)
      Files.exists(Paths.get("test_metrics_system.properties")) should be(true)
      Files.exists(Paths.get("log4j2.properties")) should be(true)
      Files.exists(Paths.get("TestUDTF.jar")) should be(true)
      Files.exists(Paths.get(notToDownload.getName)) should be(false)
      Files.delete(Paths.get("test_metrics_config.properties"))
      Files.delete(Paths.get("test_metrics_system.properties"))
      Files.delete(Paths.get("log4j2.properties"))
      Files.delete(Paths.get("TestUDTF.jar"))
    }
  }

  test("SPARK-43014: Set `spark.app.submitTime` if missing ") {
    val clArgs1 = Seq(
      "--deploy-mode", "client",
      "--master", "k8s://host:port",
      "--class", "org.SomeClass",
      "/home/thejar.jar")
    val appArgs1 = new SparkSubmitArguments(clArgs1)
    val (_, _, conf1, _) = submit.prepareSubmitEnvironment(appArgs1)
    conf1.getOption("spark.app.submitTime").isDefined should be(true)

    val submitTime = "1234567"
    val clArgs2 = Seq(
      "--deploy-mode", "client",
      "--master", "k8s://host:port",
      "--class", "org.SomeClass",
      "--conf", "spark.app.submitTime=" + submitTime,
      "/home/thejar.jar")
    val appArgs2 = new SparkSubmitArguments(clArgs2)
    val (_, _, conf2, _) = submit.prepareSubmitEnvironment(appArgs2)
    conf2.get("spark.app.submitTime") should be (submitTime)
  }

  test("SPARK-43014: Do not overwrite `spark.app.submitTime` in k8s cluster mode driver " +
    "when `spark.kubernetes.setSubmitTimeInDriver` is false") {
    val submitTime = System.currentTimeMillis() - 1000
    val clArgs = Seq(
      "--deploy-mode", "client",
      "--master", "k8s://host:port",
      "--class", "org.SomeClass",
      "--conf", "spark.app.submitTime=" + submitTime,
      "--conf", "spark.kubernetes.submitInDriver=true",
      "--conf", "spark.kubernetes.setSubmitTimeInDriver=false",
      "/home/thejar.jar")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (_, _, conf, _) = submit.prepareSubmitEnvironment(appArgs)
    conf.getLong("spark.app.submitTime", -1) should be (submitTime)
  }

  /**
   * Helper function for testing main class resolution on remote JAR files.
   *
   * @param tempDir path to temporary directory
   * @param deployMode either "client" or "cluster"
   * @return a pair of the JAR file and the 4-tuple returned by
   *         [[org.apache.spark.deploy.SparkSubmit#prepareSubmitEnvironment]]
   */
  private def testResolveMainClassOnRemoteJar(
    tempDir: File,
    deployMode: String
  ): (File, (Seq[String], Seq[String], SparkConf, String)) = {
    val excFile = TestUtils.createCompiledClass("SomeMainClass", tempDir, "", null, Seq.empty)
    val jarFile = new File(tempDir, "s3-mainClass-test-%s.jar".format(System.currentTimeMillis()))
    val jarUrl = TestUtils.createJar(
      Seq(excFile),
      jarFile,
      directoryPrefix = Some(tempDir.toString),
      mainClass = Some("SomeMainClass"))

    val hadoopConf = new Configuration()
    updateConfWithFakeS3Fs(hadoopConf)

    val clArgs = Seq(
      "--name", "testApp",
      "--master", "yarn",
      "--deploy-mode", deployMode,
      "--conf", "spark.hadoop.fs.s3a.impl=org.apache.spark.deploy.TestFileSystem",
      "--conf", "spark.hadoop.fs.s3a.impl.disable.cache=true",
      s"s3a://${jarUrl.getPath}",
      "arg1", "arg2")

    val appArgs = new SparkSubmitArguments(clArgs)
    (jarFile, submit.prepareSubmitEnvironment(appArgs, conf = Some(hadoopConf)))
  }

  test("automatically sets mainClass if primary resource is S3 JAR in client mode") {
    withTempDir { tempDir =>
      val (jarFile, (childArgs, classpaths, _, mainClass_)) = testResolveMainClassOnRemoteJar(
        tempDir, "client"
      )

      mainClass_ should be ("SomeMainClass")
      classpaths should have length 1
      classpaths.head should endWith (jarFile.getName)
      childArgs.mkString(" ") should be ("arg1 arg2")
    }
  }

  test("automatically sets mainClass if primary resource is S3 JAR in cluster mode") {
    withTempDir { tempDir =>
      val (jarFile, (childArgs, classpaths, _, mainClass_)) = testResolveMainClassOnRemoteJar(
        tempDir, "cluster"
      )

      mainClass_ should be (YARN_CLUSTER_SUBMIT_CLASS)
      classpaths should have length 1
      classpaths.head should endWith (jarFile.getName)
      childArgs.mkString(" ") should include ("--class SomeMainClass")
      childArgs.mkString(" ") should endWith ("--arg arg1 --arg arg2")
    }
  }

  test("error informatively when mainClass isn't set and S3 JAR doesn't exist") {
    val hadoopConf = new Configuration()
    updateConfWithFakeS3Fs(hadoopConf)

    val clArgs = Seq(
      "--name", "testApp",
      "--master", "yarn",
      "--conf", "spark.hadoop.fs.s3a.impl=org.apache.spark.deploy.TestFileSystem",
      "--conf", "spark.hadoop.fs.s3a.impl.disable.cache=true",
      s"s3a:///does-not-exist.jar")

    testPrematureExit(clArgs.toArray, "File /does-not-exist.jar does not exist")
  }

  test("handles confs with flag equivalents") {
    val clArgs = Seq(
      "--deploy-mode", "cluster",
      "--executor-memory", "5g",
      "--class", "org.SomeClass",
      "--conf", "spark.executor.memory=4g",
      "--conf", "spark.master=yarn",
      "thejar.jar",
      "arg1", "arg2")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (_, _, conf, mainClass) = submit.prepareSubmitEnvironment(appArgs)
    conf.get("spark.executor.memory") should be ("5g")
    conf.get("spark.master") should be ("yarn")
    conf.get(SUBMIT_DEPLOY_MODE) should be ("cluster")
    mainClass should be (SparkSubmit.YARN_CLUSTER_SUBMIT_CLASS)
  }

  test("SPARK-21568 ConsoleProgressBar should be enabled only in shells") {
    // Unset from system properties since this config is defined in the root pom's test config.
    sys.props -= UI_SHOW_CONSOLE_PROGRESS.key

    val clArgs1 = Seq("--class", "org.apache.spark.repl.Main", "spark-shell")
    val appArgs1 = new SparkSubmitArguments(clArgs1)
    val (_, _, conf1, _) = submit.prepareSubmitEnvironment(appArgs1)
    conf1.get(UI_SHOW_CONSOLE_PROGRESS) should be (true)
    var sc1: SparkContext = null
    try {
      sc1 = new SparkContext(conf1)
      assert(sc1.progressBar.isDefined)
    } finally {
      if (sc1 != null) {
        sc1.stop()
      }
    }

    val dummyJarFile = TestUtils.createJarWithClasses(Seq.empty)
    val clArgs2 = Seq("--class", "org.SomeClass", dummyJarFile.toString)
    val appArgs2 = new SparkSubmitArguments(clArgs2)
    val (_, _, conf2, _) = submit.prepareSubmitEnvironment(appArgs2)
    assert(!conf2.contains(UI_SHOW_CONSOLE_PROGRESS))
    var sc2: SparkContext = null
    try {
      sc2 = new SparkContext(conf2)
      assert(sc2.progressBar.isEmpty)
    } finally {
      if (sc2 != null) {
        sc2.stop()
      }
    }
  }

  test("launch simple application with spark-submit") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val args = Seq(
      "--class", SimpleApplicationTest.getClass.getName.stripSuffix("$"),
      "--name", "testApp",
      "--master", "local",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      unusedJar.toString)
    runSparkSubmit(args)
  }

  test("launch simple application with spark-submit with redaction") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val fileSystem = Utils.getHadoopFileSystem("/",
      SparkHadoopUtil.get.newConfiguration(new SparkConf()))
    withTempDir { testDir =>
      val testDirPath = new Path(testDir.getAbsolutePath())
      val args = Seq(
        "--class", SimpleApplicationTest.getClass.getName.stripSuffix("$"),
        "--name", "testApp",
        "--master", "local",
        "--conf", "spark.ui.enabled=false",
        "--conf", "spark.master.rest.enabled=false",
        "--conf", "spark.executorEnv.HADOOP_CREDSTORE_PASSWORD=secret_password",
        "--conf", "spark.eventLog.enabled=true",
        "--conf", "spark.eventLog.rolling.enabled=false",
        "--conf", "spark.eventLog.testing=true",
        "--conf", s"spark.eventLog.dir=${testDirPath.toUri.toString}",
        "--conf", "spark.hadoop.fs.defaultFS=unsupported://example.com",
        unusedJar.toString)
      runSparkSubmit(args)
      val listStatus = fileSystem.listStatus(testDirPath)
      val logData = EventLogFileReader.openEventLog(listStatus.last.getPath, fileSystem)
      Source.fromInputStream(logData)(Codec.UTF8).getLines().foreach { line =>
        assert(!line.contains("secret_password"))
      }
    }
  }

  test("includes jars passed in through --jars") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val jar1 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassA"))
    val jar2 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassB"))
    val jarsString = Seq(jar1, jar2).map(j => j.toString).mkString(",")
    val args = Seq(
      "--class", JarCreationTest.getClass.getName.stripSuffix("$"),
      "--name", "testApp",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--jars", jarsString,
      unusedJar.toString, "SparkSubmitClassA", "SparkSubmitClassB")
    runSparkSubmit(args)
  }

  // SPARK-7287
  test("includes jars passed in through --packages") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val main = MavenCoordinate("my.great.lib", "mylib", "0.1")
    val dep = MavenCoordinate("my.great.dep", "mylib", "0.1")
    IvyTestUtils.withRepository(main, Some(dep.toString), None) { repo =>
      val args = Seq(
        "--class", JarCreationTest.getClass.getName.stripSuffix("$"),
        "--name", "testApp",
        "--master", "local-cluster[2,1,1024]",
        "--packages", Seq(main, dep).mkString(","),
        "--repositories", repo,
        "--conf", "spark.ui.enabled=false",
        "--conf", "spark.master.rest.enabled=false",
        "--conf", s"spark.jars.ivySettings=${emptyIvySettings.getAbsolutePath()}",
        unusedJar.toString,
        "my.great.lib.MyLib", "my.great.dep.MyLib")
      runSparkSubmit(args)
    }
  }

  test("includes jars passed through spark.jars.packages and spark.jars.repositories") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val main = MavenCoordinate("my.great.lib", "mylib", "0.1")
    val dep = MavenCoordinate("my.great.dep", "mylib", "0.1")
    IvyTestUtils.withRepository(main, Some(dep.toString), None) { repo =>
      val args = Seq(
        "--class", JarCreationTest.getClass.getName.stripSuffix("$"),
        "--name", "testApp",
        "--master", "local-cluster[2,1,1024]",
        "--conf", "spark.jars.packages=my.great.lib:mylib:0.1,my.great.dep:mylib:0.1",
        "--conf", s"spark.jars.repositories=$repo",
        "--conf", "spark.ui.enabled=false",
        "--conf", "spark.master.rest.enabled=false",
        "--conf", s"spark.jars.ivySettings=${emptyIvySettings.getAbsolutePath()}",
        unusedJar.toString,
        "my.great.lib.MyLib", "my.great.dep.MyLib")
      runSparkSubmit(args)
    }
  }

  // TODO(SPARK-9603): Building a package is flaky on Jenkins Maven builds.
  // See https://gist.github.com/shivaram/3a2fecce60768a603dac for an error log
  ignore("correctly builds R packages included in a jar with --packages") {
    assume(RUtils.isRInstalled, "R isn't installed on this machine.")
    assume(RUtils.isSparkRInstalled, "SparkR is not installed in this build.")
    val main = MavenCoordinate("my.great.lib", "mylib", "0.1")
    val sparkHome = sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    val rScriptDir = Seq(
      sparkHome, "R", "pkg", "tests", "fulltests", "packageInAJarTest.R").mkString(File.separator)
    assert(new File(rScriptDir).exists)
    IvyTestUtils.withRepository(main, None, None, withR = true) { repo =>
      val args = Seq(
        "--name", "testApp",
        "--master", "local-cluster[2,1,1024]",
        "--packages", main.toString,
        "--repositories", repo,
        "--conf", s"spark.jars.ivySettings=${emptyIvySettings.getAbsolutePath()}",
        "--verbose",
        "--conf", "spark.ui.enabled=false",
        rScriptDir)
      runSparkSubmit(args)
    }
  }

  test("include an external JAR in SparkR") {
    assume(RUtils.isRInstalled, "R isn't installed on this machine.")
    val sparkHome = sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    assume(RUtils.isSparkRInstalled, "SparkR is not installed in this build.")
    val rScriptDir =
      Seq(sparkHome, "R", "pkg", "tests", "fulltests", "jarTest.R").mkString(File.separator)
    assert(new File(rScriptDir).exists)

    // compile a small jar containing a class that will be called from R code.
    withTempDir { tempDir =>
      val srcDir = new File(tempDir, "sparkrtest")
      srcDir.mkdirs()
      val excSource = new JavaSourceFromString(new File(srcDir, "DummyClass").toURI.getPath,
        """package sparkrtest;
        |
        |public class DummyClass implements java.io.Serializable {
        |  public static String helloWorld(String arg) { return "Hello " + arg; }
        |  public static int addStuff(int arg1, int arg2) { return arg1 + arg2; }
        |}
      """.
          stripMargin)
      val excFile = TestUtils.createCompiledClass("DummyClass", srcDir, excSource, Seq.empty)
      val jarFile = new File(tempDir, "sparkRTestJar-%s.jar".format(System.currentTimeMillis()))
      val jarURL = TestUtils.createJar(Seq(excFile), jarFile, directoryPrefix = Some("sparkrtest"))

      val args = Seq(
        "--name", "testApp",
        "--master", "local",
        "--jars", jarURL.toString,
        "--verbose",
        "--conf", "spark.ui.enabled=false",
        rScriptDir)
      runSparkSubmit(args)
    }
  }

  test("resolves command line argument paths correctly") {
    withTempDir { dir =>
      val archive = Paths.get(dir.toPath.toString, "single.zip")
      Files.createFile(archive)
      val jars = "/jar1,/jar2"
      val files = "local:/file1,file2"
      val archives = s"file:/archive1,${dir.toPath.toAbsolutePath.toString}/*.zip#archive3"
      val pyFiles = "py-file1,py-file2"

      // Test jars and files
      val clArgs = Seq(
        "--master", "local",
        "--class", "org.SomeClass",
        "--jars", jars,
        "--files", files,
        "thejar.jar")
      val appArgs = new SparkSubmitArguments(clArgs)
      val (_, _, conf, _) = submit.prepareSubmitEnvironment(appArgs)
      appArgs.jars should be(Utils.resolveURIs(jars))
      appArgs.files should be(Utils.resolveURIs(files))
      conf.get(JARS) should be(Utils.resolveURIs(jars + ",thejar.jar").split(",").toSeq)
      conf.get(FILES) should be(Utils.resolveURIs(files).split(",").toSeq)

      // Test files and archives (Yarn)
      val clArgs2 = Seq(
        "--master", "yarn",
        "--class", "org.SomeClass",
        "--files", files,
        "--archives", archives,
        "thejar.jar"
      )
      val appArgs2 = new SparkSubmitArguments(clArgs2)
      val (_, _, conf2, _) = submit.prepareSubmitEnvironment(appArgs2)
      appArgs2.files should be(Utils.resolveURIs(files))
      appArgs2.archives should fullyMatch regex ("file:/archive1,file:.*#archive3")
      conf2.get("spark.yarn.dist.files") should be(Utils.resolveURIs(files))
      conf2.get("spark.yarn.dist.archives") should fullyMatch regex
        ("file:/archive1,file:.*#archive3")

      // Test python files
      val clArgs3 = Seq(
        "--master", "local",
        "--py-files", pyFiles,
        "--conf", "spark.pyspark.driver.python=python3.4",
        "--conf", "spark.pyspark.python=python3.5",
        "mister.py"
      )
      val appArgs3 = new SparkSubmitArguments(clArgs3)
      val (_, _, conf3, _) = submit.prepareSubmitEnvironment(appArgs3)
      appArgs3.pyFiles should be(Utils.resolveURIs(pyFiles))
      conf3.get(SUBMIT_PYTHON_FILES) should be(
        PythonRunner.formatPaths(Utils.resolveURIs(pyFiles)))
      conf3.get(PYSPARK_DRIVER_PYTHON.key) should be("python3.4")
      conf3.get(PYSPARK_PYTHON.key) should be("python3.5")
    }
  }

  test("ambiguous archive mapping results in error message") {
    withTempDir { dir =>
      val archive1 = Paths.get(dir.toPath.toString, "first.zip")
      val archive2 = Paths.get(dir.toPath.toString, "second.zip")
      Files.createFile(archive1)
      Files.createFile(archive2)
      val jars = "/jar1,/jar2"
      val files = "local:/file1,file2"
      val archives = s"file:/archive1,${dir.toPath.toAbsolutePath.toString}/*.zip#archive3"
      val pyFiles = "py-file1,py-file2"

      // Test files and archives (Yarn)
      val clArgs2 = Seq(
        "--master", "yarn",
        "--class", "org.SomeClass",
        "--files", files,
        "--archives", archives,
        "thejar.jar"
      )

      testPrematureExit(clArgs2.toArray, "resolves ambiguously to multiple files")
    }
  }

  test("resolves config paths correctly") {
    val jars = "/jar1,/jar2" // spark.jars
    val files = "local:/file1,file2" // spark.files / spark.yarn.dist.files
    val archives = "file:/archive1,archive2" // spark.yarn.dist.archives
    val pyFiles = "py-file1,py-file2" // spark.submit.pyFiles

    withTempDir { tmpDir =>
      // Test jars and files
      val f1 = File.createTempFile("test-submit-jars-files", "", tmpDir)
      Utils.tryWithResource(new PrintWriter(f1)) { writer =>
        writer.println("spark.jars " + jars)
        writer.println("spark.files " + files)
      }
      val clArgs = Seq(
        "--master", "local",
        "--class", "org.SomeClass",
        "--properties-file", f1.getPath,
        "thejar.jar"
      )
      val appArgs = new SparkSubmitArguments(clArgs)
      val (_, _, conf, _) = submit.prepareSubmitEnvironment(appArgs)
      conf.get(JARS) should be(Utils.resolveURIs(jars + ",thejar.jar").split(",").toSeq)
      conf.get(FILES) should be(Utils.resolveURIs(files).split(",").toSeq)

      // Test files and archives (Yarn)
      val f2 = File.createTempFile("test-submit-files-archives", "", tmpDir)
      Utils.tryWithResource(new PrintWriter(f2)) { writer =>
        writer.println("spark.yarn.dist.files " + files)
        writer.println("spark.yarn.dist.archives " + archives)
      }
      val clArgs2 = Seq(
        "--master", "yarn",
        "--class", "org.SomeClass",
        "--properties-file", f2.getPath,
        "thejar.jar"
      )
      val appArgs2 = new SparkSubmitArguments(clArgs2)
      val (_, _, conf2, _) = submit.prepareSubmitEnvironment(appArgs2)
      conf2.get("spark.yarn.dist.files") should be(Utils.resolveURIs(files))
      conf2.get("spark.yarn.dist.archives") should be(Utils.resolveURIs(archives))

      // Test python files
      val f3 = File.createTempFile("test-submit-python-files", "", tmpDir)
      Utils.tryWithResource(new PrintWriter(f3)) { writer =>
        writer.println("spark.submit.pyFiles " + pyFiles)
      }
      val clArgs3 = Seq(
        "--master", "local",
        "--properties-file", f3.getPath,
        "mister.py"
      )
      val appArgs3 = new SparkSubmitArguments(clArgs3)
      val (_, _, conf3, _) = submit.prepareSubmitEnvironment(appArgs3)
      conf3.get(SUBMIT_PYTHON_FILES) should be(
        PythonRunner.formatPaths(Utils.resolveURIs(pyFiles)))

      // Test remote python files
      val hadoopConf = new Configuration()
      updateConfWithFakeS3Fs(hadoopConf)
      val f4 = File.createTempFile("test-submit-remote-python-files", "", tmpDir)
      val pyFile1 = File.createTempFile("file1", ".py", tmpDir)
      val pyFile2 = File.createTempFile("file2", ".py", tmpDir)
      val remotePyFiles = s"s3a://${pyFile1.getAbsolutePath},s3a://${pyFile2.getAbsolutePath}"
      Utils.tryWithResource(new PrintWriter(f4)) { writer =>
        writer.println("spark.submit.pyFiles " + remotePyFiles)
      }
      val clArgs4 = Seq(
        "--master", "yarn",
        "--deploy-mode", "cluster",
        "--properties-file", f4.getPath,
        "hdfs:///tmp/mister.py"
      )
      val appArgs4 = new SparkSubmitArguments(clArgs4)
      val (_, _, conf4, _) = submit.prepareSubmitEnvironment(appArgs4, conf = Some(hadoopConf))
      // Should not format python path for yarn cluster mode
      conf4.get(SUBMIT_PYTHON_FILES) should be(Utils.resolveURIs(remotePyFiles).split(","))
    }
  }

  test("user classpath first in driver") {
    val systemJar = TestUtils.createJarWithFiles(Map("test.resource" -> "SYSTEM"))
    val userJar = TestUtils.createJarWithFiles(Map("test.resource" -> "USER"))
    val args = Seq(
      "--class", UserClasspathFirstTest.getClass.getName.stripSuffix("$"),
      "--name", "testApp",
      "--master", "local",
      "--conf", "spark.driver.extraClassPath=" + systemJar,
      "--conf", "spark.driver.userClassPathFirst=true",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      userJar.toString)
    runSparkSubmit(args)
  }

  test("SPARK_CONF_DIR overrides spark-defaults.conf") {
    forConfDir(Map("spark.executor.memory" -> "3g")) { path =>
      val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
      val args = Seq(
        "--class", SimpleApplicationTest.getClass.getName.stripSuffix("$"),
        "--name", "testApp",
        "--master", "local",
        unusedJar.toString)
      val appArgs = new SparkSubmitArguments(args, env = Map("SPARK_CONF_DIR" -> path))
      appArgs.executorMemory should be ("3g")
    }
  }

  test("SPARK-48392: load spark-defaults.conf when --load-spark-defaults is set") {
    forConfDir(Map("spark.executor.memory" -> "3g", "spark.driver.memory" -> "3g")) { path =>
      withPropertyFile("spark-conf.properties",
          Map("spark.executor.cores" -> "16", "spark.driver.memory" -> "4g")) { propsFile =>
        val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
        val args = Seq(
          "--class", SimpleApplicationTest.getClass.getName.stripSuffix("$"),
          "--name", "testApp",
          "--master", "local",
          "--properties-file", propsFile,
          "--load-spark-defaults",
          unusedJar.toString)
        val appArgs = new SparkSubmitArguments(args, env = Map("SPARK_CONF_DIR" -> path))
        appArgs.executorCores should be("16")
        appArgs.executorMemory should be("3g")
        appArgs.driverMemory should be("4g")
      }
    }
  }

  test("SPARK-48392: should skip spark-defaults.conf when --load-spark-defaults is not set") {
    forConfDir(Map("spark.executor.memory" -> "3g", "spark.driver.memory" -> "3g")) { path =>
      withPropertyFile("spark-conf.properties",
        Map("spark.executor.cores" -> "16", "spark.driver.memory" -> "4g")) { propsFile =>
        val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
        val args = Seq(
          "--class", SimpleApplicationTest.getClass.getName.stripSuffix("$"),
          "--name", "testApp",
          "--master", "local",
          "--properties-file", propsFile,
          unusedJar.toString)
        val appArgs = new SparkSubmitArguments(args, env = Map("SPARK_CONF_DIR" -> path))
        appArgs.executorCores should be("16")
        appArgs.driverMemory should be("4g")
        appArgs.executorMemory should be(null)
      }
    }
  }

  test("support glob path") {
    withTempDir { tmpJarDir =>
      withTempDir { tmpFileDir =>
        withTempDir { tmpPyFileDir =>
          withTempDir { tmpArchiveDir =>
            val jar1 = TestUtils.createJarWithFiles(Map("test.resource" -> "1"), tmpJarDir)
            val jar2 = TestUtils.createJarWithFiles(Map("test.resource" -> "USER"), tmpJarDir)

            val file1 = File.createTempFile("tmpFile1", "", tmpFileDir)
            val file2 = File.createTempFile("tmpFile2", "", tmpFileDir)

            val pyFile1 = File.createTempFile("tmpPy1", ".py", tmpPyFileDir)
            val pyFile2 = File.createTempFile("tmpPy2", ".egg", tmpPyFileDir)

            val archive1 = File.createTempFile("archive1", ".zip", tmpArchiveDir)
            val archive2 = File.createTempFile("archive2", ".zip", tmpArchiveDir)

            val tempPyFile = File.createTempFile("tmpApp", ".py")
            tempPyFile.deleteOnExit()

            val args = Seq(
              "--class", UserClasspathFirstTest.getClass.getName.stripPrefix("$"),
              "--name", "testApp",
              "--master", "yarn",
              "--deploy-mode", "client",
              "--jars", s"${tmpJarDir.getAbsolutePath}/*.jar",
              "--files", s"${tmpFileDir.getAbsolutePath}/tmpFile*",
              "--py-files", s"${tmpPyFileDir.getAbsolutePath}/tmpPy*",
              "--archives", s"${tmpArchiveDir.getAbsolutePath}/*.zip",
              tempPyFile.toURI().toString())

            val appArgs = new SparkSubmitArguments(args)
            val (_, _, conf, _) = submit.prepareSubmitEnvironment(appArgs)
            conf.get("spark.yarn.dist.jars").split(",").toSet should be
            (Set(jar1.toURI.toString, jar2.toURI.toString))
            conf.get("spark.yarn.dist.files").split(",").toSet should be
            (Set(file1.toURI.toString, file2.toURI.toString))
            conf.get("spark.yarn.dist.pyFiles").split(",").toSet should be
            (Set(pyFile1.getAbsolutePath, pyFile2.getAbsolutePath))
            conf.get("spark.yarn.dist.archives").split(",").toSet should be
            (Set(archive1.toURI.toString, archive2.toURI.toString))
          }
        }
      }
    }
  }

  test("SPARK-27575: yarn confs should merge new value with existing value") {
    val tmpJarDir = Utils.createTempDir()
    val jar1 = TestUtils.createJarWithFiles(Map("test.resource" -> "1"), tmpJarDir)
    val jar2 = TestUtils.createJarWithFiles(Map("test.resource" -> "USER"), tmpJarDir)

    val tmpJarDirYarnOpt = Utils.createTempDir()
    val jar1YarnOpt = TestUtils.createJarWithFiles(Map("test.resource" -> "2"), tmpJarDirYarnOpt)
    val jar2YarnOpt = TestUtils.createJarWithFiles(Map("test.resource" -> "USER2"),
      tmpJarDirYarnOpt)

    val tmpFileDir = Utils.createTempDir()
    val file1 = File.createTempFile("tmpFile1", "", tmpFileDir)
    val file2 = File.createTempFile("tmpFile2", "", tmpFileDir)

    val tmpFileDirYarnOpt = Utils.createTempDir()
    val file1YarnOpt = File.createTempFile("tmpPy1YarnOpt", ".py", tmpFileDirYarnOpt)
    val file2YarnOpt = File.createTempFile("tmpPy2YarnOpt", ".egg", tmpFileDirYarnOpt)

    val tmpPyFileDir = Utils.createTempDir()
    val pyFile1 = File.createTempFile("tmpPy1", ".py", tmpPyFileDir)
    val pyFile2 = File.createTempFile("tmpPy2", ".egg", tmpPyFileDir)

    val tmpPyFileDirYarnOpt = Utils.createTempDir()
    val pyFile1YarnOpt = File.createTempFile("tmpPy1YarnOpt", ".py", tmpPyFileDirYarnOpt)
    val pyFile2YarnOpt = File.createTempFile("tmpPy2YarnOpt", ".egg", tmpPyFileDirYarnOpt)

    val tmpArchiveDir = Utils.createTempDir()
    val archive1 = File.createTempFile("archive1", ".zip", tmpArchiveDir)
    val archive2 = File.createTempFile("archive2", ".zip", tmpArchiveDir)

    val tmpArchiveDirYarnOpt = Utils.createTempDir()
    val archive1YarnOpt = File.createTempFile("archive1YarnOpt", ".zip", tmpArchiveDirYarnOpt)
    val archive2YarnOpt = File.createTempFile("archive2YarnOpt", ".zip", tmpArchiveDirYarnOpt)

    val tempPyFile = File.createTempFile("tmpApp", ".py")
    tempPyFile.deleteOnExit()

    val args = Seq(
      "--class", UserClasspathFirstTest.getClass.getName.stripPrefix("$"),
      "--name", "testApp",
      "--master", "yarn",
      "--deploy-mode", "client",
      "--jars", s"${tmpJarDir.getAbsolutePath}/*.jar",
      "--files", s"${tmpFileDir.getAbsolutePath}/tmpFile*",
      "--py-files", s"${tmpPyFileDir.getAbsolutePath}/tmpPy*",
      "--archives", s"${tmpArchiveDir.getAbsolutePath}/*.zip",
      "--conf", "spark.yarn.dist.files=" +
        s"${Seq(file1YarnOpt, file2YarnOpt).map(_.toURI.toString).mkString(",")}",
      "--conf", "spark.yarn.dist.pyFiles=" +
        s"${Seq(pyFile1YarnOpt, pyFile2YarnOpt).map(_.toURI.toString).mkString(",")}",
      "--conf", "spark.yarn.dist.jars=" +
        s"${Seq(jar1YarnOpt, jar2YarnOpt).map(_.toURI.toString).mkString(",")}",
      "--conf", "spark.yarn.dist.archives=" +
        s"${Seq(archive1YarnOpt, archive2YarnOpt).map(_.toURI.toString).mkString(",")}",
      tempPyFile.toURI().toString())

    def assertEqualsWithURLs(expected: Set[URL], confValue: String): Unit = {
      val confValPaths = confValue.split(",").map(new Path(_)).toSet
      assert(expected.map(u => new Path(u.toURI)) === confValPaths)
    }

    def assertEqualsWithFiles(expected: Set[File], confValue: String): Unit = {
      assertEqualsWithURLs(expected.map(_.toURI.toURL), confValue)
    }

    val appArgs = new SparkSubmitArguments(args)
    val (_, _, conf, _) = submit.prepareSubmitEnvironment(appArgs)
    assertEqualsWithURLs(
      Set(jar1, jar2, jar1YarnOpt, jar2YarnOpt), conf.get("spark.yarn.dist.jars"))
    assertEqualsWithFiles(
      Set(file1, file2, file1YarnOpt, file2YarnOpt), conf.get("spark.yarn.dist.files"))
    assertEqualsWithFiles(
      Set(pyFile1, pyFile2, pyFile1YarnOpt, pyFile2YarnOpt), conf.get("spark.yarn.dist.pyFiles"))
    assertEqualsWithFiles(Set(archive1, archive2, archive1YarnOpt, archive2YarnOpt),
      conf.get("spark.yarn.dist.archives"))
  }

  // scalastyle:on println

  private def checkDownloadedFile(sourcePath: String, outputPath: String): Unit = {
    if (sourcePath == outputPath) {
      return
    }

    val sourceUri = new URI(sourcePath)
    val outputUri = new URI(outputPath)
    assert(outputUri.getScheme === "file")

    // The path and filename are preserved.
    assert(outputUri.getPath.endsWith(new Path(sourceUri).getName))
    assert(FileUtils.readFileToString(new File(outputUri.getPath), StandardCharsets.UTF_8) ===
      FileUtils.readFileToString(new File(sourceUri.getPath), StandardCharsets.UTF_8))
  }

  private def deleteTempOutputFile(outputPath: String): Unit = {
    val outputFile = new File(new URI(outputPath).getPath)
    if (outputFile.exists) {
      outputFile.delete()
    }
  }

  test("downloadFile - invalid url") {
    val sparkConf = new SparkConf(false)
    intercept[IOException] {
      DependencyUtils.downloadFile(
        "abc:/my/file", Utils.createTempDir(), sparkConf, new Configuration())
    }
  }

  test("downloadFile - file doesn't exist") {
    val sparkConf = new SparkConf(false)
    val hadoopConf = new Configuration()
    val tmpDir = Utils.createTempDir()
    updateConfWithFakeS3Fs(hadoopConf)
    intercept[FileNotFoundException] {
      DependencyUtils.downloadFile("s3a:/no/such/file", tmpDir, sparkConf, hadoopConf)
    }
  }

  test("downloadFile does not download local file") {
    val sparkConf = new SparkConf(false)
    // empty path is considered as local file.
    val tmpDir = Files.createTempDirectory("tmp").toFile
    assert(DependencyUtils.downloadFile("", tmpDir, sparkConf, new Configuration()) === "")
    assert(DependencyUtils.downloadFile(
      "/local/file", tmpDir, sparkConf, new Configuration()) === "/local/file")
  }

  test("download one file to local") {
    val sparkConf = new SparkConf(false)
    val jarFile = File.createTempFile("test", ".jar")
    jarFile.deleteOnExit()
    val content = "hello, world"
    FileUtils.write(jarFile, content, StandardCharsets.UTF_8)
    val hadoopConf = new Configuration()
    val tmpDir = Files.createTempDirectory("tmp").toFile
    updateConfWithFakeS3Fs(hadoopConf)
    val sourcePath = s"s3a://${jarFile.toURI.getPath}"
    val outputPath = DependencyUtils.downloadFile(sourcePath, tmpDir, sparkConf, hadoopConf)
    checkDownloadedFile(sourcePath, outputPath)
    deleteTempOutputFile(outputPath)
  }

  test("download list of files to local") {
    val sparkConf = new SparkConf(false)
    val jarFile = File.createTempFile("test", ".jar")
    jarFile.deleteOnExit()
    val content = "hello, world"
    FileUtils.write(jarFile, content, StandardCharsets.UTF_8)
    val hadoopConf = new Configuration()
    val tmpDir = Files.createTempDirectory("tmp").toFile
    updateConfWithFakeS3Fs(hadoopConf)
    val sourcePaths = Seq("/local/file", s"s3a://${jarFile.toURI.getPath}")
    val outputPaths = DependencyUtils
      .downloadFileList(sourcePaths.mkString(","), tmpDir, sparkConf, hadoopConf)
      .split(",")

    assert(outputPaths.length === sourcePaths.length)
    sourcePaths.zip(outputPaths).foreach { case (sourcePath, outputPath) =>
      checkDownloadedFile(sourcePath, outputPath)
      deleteTempOutputFile(outputPath)
    }
  }

  test("remove copies of application jar from classpath") {
    val fs = File.separator
    val sparkConf = new SparkConf(false)
    val hadoopConf = new Configuration()

    val appJarName = "myApp.jar"
    val jar1Name = "myJar1.jar"
    val jar2Name = "myJar2.jar"
    val userJar = s"file:/path${fs}to${fs}app${fs}jar$fs$appJarName"
    val jars = s"file:/$jar1Name,file:/$appJarName,file:/$jar2Name"

    val resolvedJars = DependencyUtils.resolveAndDownloadJars(jars, userJar, sparkConf, hadoopConf)

    assert(!resolvedJars.contains(appJarName))
    assert(resolvedJars.contains(jar1Name) && resolvedJars.contains(jar2Name))
  }

  test("Avoid re-upload remote resources in yarn client mode") {
    val hadoopConf = new Configuration()
    updateConfWithFakeS3Fs(hadoopConf)

    withTempDir { tmpDir =>
      val file = File.createTempFile("tmpFile", "", tmpDir)
      val pyFile = File.createTempFile("tmpPy", ".egg", tmpDir)
      val mainResource = File.createTempFile("tmpPy", ".py", tmpDir)
      val tmpJar = TestUtils.createJarWithFiles(Map("test.resource" -> "USER"), tmpDir)
      val tmpJarPath = s"s3a://${new File(tmpJar.toURI).getAbsolutePath}"

      val args = Seq(
        "--class", UserClasspathFirstTest.getClass.getName.stripPrefix("$"),
        "--name", "testApp",
        "--master", "yarn",
        "--deploy-mode", "client",
        "--jars", tmpJarPath,
        "--files", s"s3a://${file.getAbsolutePath}",
        "--py-files", s"s3a://${pyFile.getAbsolutePath}",
        s"s3a://$mainResource"
      )

      val appArgs = new SparkSubmitArguments(args)
      val (_, _, conf, _) = submit.prepareSubmitEnvironment(appArgs, conf = Some(hadoopConf))

      // All the resources should still be remote paths, so that YARN client will not upload again.
      conf.get("spark.yarn.dist.jars") should be(tmpJarPath)
      conf.get("spark.yarn.dist.files") should be(s"s3a://${file.getAbsolutePath}")
      conf.get("spark.yarn.dist.pyFiles") should be(s"s3a://${pyFile.getAbsolutePath}")

      // Local repl jars should be a local path.
      conf.get("spark.repl.local.jars") should (startWith("file:"))

      // local py files should not be a URI format.
      conf.get(SUBMIT_PYTHON_FILES).foreach { _ should (startWith("/")) }
    }
  }

  test("download remote resource if it is not supported by yarn service") {
    testRemoteResources(enableHttpFs = false)
  }

  test("avoid downloading remote resource if it is supported by yarn service") {
    testRemoteResources(enableHttpFs = true)
  }

  test("force download from forced schemes") {
    testRemoteResources(enableHttpFs = true, forceDownloadSchemes = Seq("http"))
  }

  test("force download for all the schemes") {
    testRemoteResources(enableHttpFs = true, forceDownloadSchemes = Seq("*"))
  }

  test("SPARK-32119: Jars and files should be loaded when Executors launch for plugins") {
    val tempDir = Utils.createTempDir()
    val tempFileName = "test.txt"
    val tempFile = new File(tempDir, tempFileName)

    // scalastyle:off println
    Utils.tryWithResource {
      new PrintWriter(tempFile)
    } { writer =>
      writer.println("SparkPluginTest")
    }
    // scalastyle:on println

    val sparkPluginCodeBody =
      """
        |@Override
        |public org.apache.spark.api.plugin.ExecutorPlugin executorPlugin() {
        |  return new TestExecutorPlugin();
        |}
        |
        |@Override
        |public org.apache.spark.api.plugin.DriverPlugin driverPlugin() { return null; }
      """.stripMargin
    val executorPluginCodeBody =
      s"""
        |@Override
        |public void init(
        |    org.apache.spark.api.plugin.PluginContext ctx,
        |    java.util.Map<String, String> extraConf) {
        |  String str = null;
        |  try (java.io.BufferedReader reader =
        |    new java.io.BufferedReader(new java.io.InputStreamReader(
        |      new java.io.FileInputStream("$tempFileName")))) {
        |    str = reader.readLine();
        |  } catch (java.io.IOException e) {
        |    throw new RuntimeException(e);
        |  } finally {
        |    assert str == "SparkPluginTest";
        |  }
        |}
      """.stripMargin

    val compiledExecutorPlugin = TestUtils.createCompiledClass(
      "TestExecutorPlugin",
      tempDir,
      "",
      null,
      Seq.empty,
      Seq("org.apache.spark.api.plugin.ExecutorPlugin"),
      executorPluginCodeBody)

    val thisClassPath =
      sys.props("java.class.path").split(File.pathSeparator).map(p => new File(p).toURI.toURL)
    val compiledSparkPlugin = TestUtils.createCompiledClass(
      "TestSparkPlugin",
      tempDir,
      "",
      null,
      Seq(tempDir.toURI.toURL) ++ thisClassPath,
      Seq("org.apache.spark.api.plugin.SparkPlugin"),
      sparkPluginCodeBody)

    val jarUrl = TestUtils.createJar(
      Seq(compiledSparkPlugin, compiledExecutorPlugin),
      new File(tempDir, "testplugin.jar"))

    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val unusedFile = Files.createTempFile(tempDir.toPath, "unused", null)
    val args = Seq(
      "--class", SimpleApplicationTest.getClass.getName.stripSuffix("$"),
      "--name", "testApp",
      "--master", "local-cluster[1,1,1024]",
      "--conf", "spark.plugins=TestSparkPlugin",
      "--conf", "spark.ui.enabled=false",
      "--jars", jarUrl.toString + "," + unusedJar.toString,
      "--files", tempFile.toString + "," + unusedFile.toString,
      unusedJar.toString)
    runSparkSubmit(args)
  }

  test("SPARK-45762: The ShuffleManager plugin to use can be defined in a user jar") {
    val shuffleManagerBody = """
      |@Override
      |public <K, V, C> org.apache.spark.shuffle.ShuffleHandle registerShuffle(
      |    int shuffleId,
      |    org.apache.spark.ShuffleDependency<K, V, C> dependency) {
      |  throw new java.lang.UnsupportedOperationException("This is a test ShuffleManager!");
      |}
      |
      |@Override
      |public <K, V> org.apache.spark.shuffle.ShuffleWriter<K, V> getWriter(
      |    org.apache.spark.shuffle.ShuffleHandle handle,
      |    long mapId,
      |    org.apache.spark.TaskContext context,
      |    org.apache.spark.shuffle.ShuffleWriteMetricsReporter metrics) {
      |  throw new java.lang.UnsupportedOperationException("This is a test ShuffleManager!");
      |}
      |
      |@Override
      |public <K, C> org.apache.spark.shuffle.ShuffleReader<K, C> getReader(
      |    org.apache.spark.shuffle.ShuffleHandle handle,
      |    int startMapIndex,
      |    int endMapIndex,
      |    int startPartition,
      |    int endPartition,
      |    org.apache.spark.TaskContext context,
      |    org.apache.spark.shuffle.ShuffleReadMetricsReporter metrics) {
      |  throw new java.lang.UnsupportedOperationException("This is a test ShuffleManager!");
      |}
      |
      |@Override
      |public boolean unregisterShuffle(int shuffleId) {
      |  throw new java.lang.UnsupportedOperationException("This is a test ShuffleManager!");
      |}
      |
      |@Override
      |public org.apache.spark.shuffle.ShuffleBlockResolver shuffleBlockResolver() {
      |  throw new java.lang.UnsupportedOperationException("This is a test ShuffleManager!");
      |}
      |
      |@Override
      |public void stop() {
      |}
  """.stripMargin

    val tempDir = Utils.createTempDir()
    val compiledShuffleManager = TestUtils.createCompiledClass(
      "TestShuffleManager",
      tempDir,
      "",
      null,
      Seq.empty,
      Seq("org.apache.spark.shuffle.ShuffleManager"),
      shuffleManagerBody)

    val jarUrl = TestUtils.createJar(
      Seq(compiledShuffleManager),
      new File(tempDir, "testplugin.jar"))

    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val argsBase = Seq(
      "--class", SimpleApplicationTest.getClass.getName.stripSuffix("$"),
      "--name", "testApp",
      "--master", "local-cluster[1,1,1024]",
      "--conf", "spark.shuffle.manager=TestShuffleManager",
      "--conf", "spark.ui.enabled=false")

    val argsError = argsBase :+ unusedJar.toString
    // check process error exit code
    assertResult(1)(runSparkSubmit(argsError, expectFailure = true))

    val argsSuccess = (argsBase ++ Seq("--jars", jarUrl.toString)) :+ unusedJar.toString
    // check process success exit code
    assertResult(0)(
      runSparkSubmit(argsSuccess, expectFailure = false))
  }

  private def testRemoteResources(
      enableHttpFs: Boolean,
      forceDownloadSchemes: Seq[String] = Nil): Unit = {
    val hadoopConf = new Configuration()
    updateConfWithFakeS3Fs(hadoopConf)
    if (enableHttpFs) {
      hadoopConf.set("fs.http.impl", classOf[TestFileSystem].getCanonicalName)
    } else {
      hadoopConf.set("fs.http.impl", getClass().getName() + ".DoesNotExist")
    }
    hadoopConf.set("fs.http.impl.disable.cache", "true")

    val tmpDir = Utils.createTempDir()
    val mainResource = File.createTempFile("tmpPy", ".py", tmpDir)
    val tmpS3Jar = TestUtils.createJarWithFiles(Map("test.resource" -> "USER"), tmpDir)
    val tmpS3JarPath = s"s3a://${new File(tmpS3Jar.toURI).getAbsolutePath}"
    val tmpHttpJar = TestUtils.createJarWithFiles(Map("test.resource" -> "USER"), tmpDir)
    val tmpHttpJarPath = s"http://${new File(tmpHttpJar.toURI).getAbsolutePath}"

    val forceDownloadArgs = if (forceDownloadSchemes.nonEmpty) {
      Seq("--conf", s"spark.yarn.dist.forceDownloadSchemes=${forceDownloadSchemes.mkString(",")}")
    } else {
      Nil
    }

    val args = Seq(
      "--class", UserClasspathFirstTest.getClass.getName.stripPrefix("$"),
      "--name", "testApp",
      "--master", "yarn",
      "--deploy-mode", "client",
      "--jars", s"$tmpS3JarPath,$tmpHttpJarPath"
    ) ++ forceDownloadArgs ++ Seq(s"s3a://$mainResource")

    val appArgs = new SparkSubmitArguments(args)
    val (_, _, conf, _) = submit.prepareSubmitEnvironment(appArgs, conf = Some(hadoopConf))

    val jars = conf.get("spark.yarn.dist.jars").split(",").toSet

    def isSchemeForcedDownload(scheme: String) = {
      forceDownloadSchemes.contains("*") || forceDownloadSchemes.contains(scheme)
    }

    if (!isSchemeForcedDownload("s3")) {
      assert(jars.contains(tmpS3JarPath))
    }

    if (enableHttpFs && forceDownloadSchemes.isEmpty) {
      // If Http FS is supported by yarn service, the URI of remote http resource should
      // still be remote.
      assert(jars.contains(tmpHttpJarPath))
    } else if (!enableHttpFs || isSchemeForcedDownload("http")) {
      // If Http FS is not supported by yarn service, or http scheme is configured to be force
      // downloading, the URI of remote http resource should be changed to a local one.
      val jarName = new File(tmpHttpJar.toURI).getName
      val localHttpJar = jars.filter(_.contains(jarName))
      localHttpJar.size should be(1)
      localHttpJar.head should startWith("file:")
    }
  }

  private def forConfDir(defaults: Map[String, String]) (f: String => Unit) = {
    withTempDir { tmpDir =>
      val defaultsConf = new File(tmpDir.getAbsolutePath, "spark-defaults.conf")
      val writer =
        new OutputStreamWriter(new FileOutputStream(defaultsConf), StandardCharsets.UTF_8)
      for ((key, value) <- defaults) writer.write(s"$key $value\n")
      writer.close()
      f(tmpDir.getAbsolutePath)
    }
  }

  private def withPropertyFile(fileName: String, conf: Map[String, String])(f: String => Unit) = {
    withTempDir { tmpDir =>
      val props = new java.util.Properties()
      val propsFile = File.createTempFile(fileName, "", tmpDir)
      val propsOutputStream = new FileOutputStream(propsFile)
      try {
        conf.foreach { case (k, v) => props.put(k, v) }
        props.store(propsOutputStream, "")
      } finally {
        propsOutputStream.close()
      }

      f(propsFile.getPath)
    }
  }

  private def updateConfWithFakeS3Fs(conf: Configuration): Unit = {
    conf.set("fs.s3a.impl", classOf[TestFileSystem].getCanonicalName)
    conf.set("fs.s3a.impl.disable.cache", "true")
  }

  test("start SparkApplication without modifying system properties") {
    val args = Array(
      "--class", classOf[TestSparkApplication].getName(),
      "--master", "local",
      "--conf", "spark.test.hello=world",
      "spark-internal",
      "hello")

    val exception = intercept[SparkException] {
      submit.doSubmit(args)
    }

    assert(exception.getMessage() === "hello")
  }

  test("support --py-files/spark.submit.pyFiles in non pyspark application") {
    val hadoopConf = new Configuration()
    updateConfWithFakeS3Fs(hadoopConf)

    withTempDir { tmpDir =>
      val pyFile = File.createTempFile("tmpPy", ".egg", tmpDir)

      val args = Seq(
        "--class", UserClasspathFirstTest.getClass.getName.stripPrefix("$"),
        "--name", "testApp",
        "--master", "yarn",
        "--deploy-mode", "client",
        "--py-files", s"s3a://${pyFile.getAbsolutePath}",
        "spark-internal"
      )

      val appArgs = new SparkSubmitArguments(args)
      val (_, _, conf, _) = submit.prepareSubmitEnvironment(appArgs, conf = Some(hadoopConf))

      conf.get(PY_FILES.key) should be(s"s3a://${pyFile.getAbsolutePath}")
      conf.get(SUBMIT_PYTHON_FILES).foreach { _ should (startWith("/")) }

      // Verify "spark.submit.pyFiles"
      val args1 = Seq(
        "--class", UserClasspathFirstTest.getClass.getName.stripPrefix("$"),
        "--name", "testApp",
        "--master", "yarn",
        "--deploy-mode", "client",
        "--conf", s"spark.submit.pyFiles=s3a://${pyFile.getAbsolutePath}",
        "spark-internal"
      )

      val appArgs1 = new SparkSubmitArguments(args1)
      val (_, _, conf1, _) = submit.prepareSubmitEnvironment(appArgs1, conf = Some(hadoopConf))

      conf1.get(PY_FILES.key) should be(s"s3a://${pyFile.getAbsolutePath}")
      conf.get(SUBMIT_PYTHON_FILES).foreach { _ should (startWith("/")) }
    }
  }

  test("handles natural line delimiters in --properties-file and --conf uniformly") {
    val delimKey = "spark.my.delimiter."
    val LF = "\n"
    val CR = "\r"

    val lineFeedFromCommandLine = s"${delimKey}lineFeedFromCommandLine" -> LF
    val leadingDelimKeyFromFile = s"${delimKey}leadingDelimKeyFromFile" -> s"${LF}blah"
    val trailingDelimKeyFromFile = s"${delimKey}trailingDelimKeyFromFile" -> s"blah${CR}"
    val infixDelimFromFile = s"${delimKey}infixDelimFromFile" -> s"${CR}blah${LF}"
    val nonDelimSpaceFromFile = s"${delimKey}nonDelimSpaceFromFile" -> " blah\f"

    val testProps = Map(leadingDelimKeyFromFile, trailingDelimKeyFromFile, infixDelimFromFile,
      nonDelimSpaceFromFile)

    withPropertyFile("test-spark-conf.properties", testProps) { propsFile =>
      val clArgs = Seq(
        "--class", "org.SomeClass",
        "--conf", s"${lineFeedFromCommandLine._1}=${lineFeedFromCommandLine._2}",
        "--conf", "spark.master=yarn",
        "--properties-file", propsFile,
        "thejar.jar")

      val appArgs = new SparkSubmitArguments(clArgs)
      val (_, _, conf, _) = submit.prepareSubmitEnvironment(appArgs)

      Seq(
        lineFeedFromCommandLine,
        leadingDelimKeyFromFile,
        trailingDelimKeyFromFile,
        infixDelimFromFile
      ).foreach { case (k, v) =>
        conf.get(k) should be (v)
      }

      conf.get(nonDelimSpaceFromFile._1) should be ("blah")
    }
  }

  test("get a Spark configuration from arguments") {
    val testConf = "spark.test.hello" -> "world"
    val masterConf = "spark.master" -> "yarn"
    val clArgs = Seq(
      "--conf", s"${testConf._1}=${testConf._2}",
      "--conf", s"${masterConf._1}=${masterConf._2}",
      "--class", "Foo",
      "app.jar")
    val conf = new SparkSubmitArguments(clArgs).toSparkConf()
    Seq(
      testConf,
      masterConf
    ).foreach { case (k, v) =>
      conf.get(k) should be (v)
    }
  }

  test("SPARK-43540: Add working directory into classpath on the driver in K8S cluster mode") {
    val clArgs = Seq(
      "--deploy-mode", "client",
      "--master", "k8s://host:port",
      "--class", "org.SomeClass",
      "--conf", "spark.kubernetes.submitInDriver=true",
      "/home/thejar.jar")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (_, classpath, _, _) = submit.prepareSubmitEnvironment(appArgs)
    assert(classpath.contains("."))
  }

  // Requires Python dependencies for Spark Connect. Should be enabled by default.
  ignore("Spark Connect application submission (Python)") {
    val pyFile = File.createTempFile("remote_test", ".py")
    pyFile.deleteOnExit()
    val content =
      "from pyspark.sql import SparkSession;" +
        "spark = SparkSession.builder.getOrCreate();" +
        "assert 'connect' in str(type(spark));" +
        "assert spark.range(1).first()[0] == 0"
    FileUtils.write(pyFile, content, StandardCharsets.UTF_8)
    val args = Seq(
      "--name", "testPyApp",
      "--remote", "local",
      pyFile.getAbsolutePath)
    runSparkSubmit(args)
  }
}

object JarCreationTest extends Logging {
  def main(args: Array[String]): Unit = {
    TestUtils.configTestLog4j2("INFO")
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val result = sc.makeRDD(1 to 100, 10).mapPartitions { x =>
      var exception: String = null
      try {
        Utils.classForName(args(0))
        Utils.classForName(args(1))
      } catch {
        case t: Throwable =>
          exception = t.toString + "\n" + Utils.exceptionString(t)
          exception = exception.replaceAll("\n", "\n\t")
      }
      Option(exception).toSeq.iterator
    }.collect()
    if (result.nonEmpty) {
      throw new Exception("Could not load user class from jar:\n" + result(0))
    }
    sc.stop()
  }
}

object SimpleApplicationTest {
  def main(args: Array[String]): Unit = {
    TestUtils.configTestLog4j2("INFO")
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val configs = Seq("spark.master", "spark.app.name")
    for (config <- configs) {
      val masterValue = conf.get(config)
      val executorValues = sc
        .makeRDD(1 to 100, 10)
        .map(x => SparkEnv.get.conf.get(config))
        .collect()
        .distinct
      if (executorValues.length != 1) {
        throw new SparkException(s"Inconsistent values for $config: " +
          s"${executorValues.mkString("values(", ", ", ")")}")
      }
      val executorValue = executorValues(0)
      if (executorValue != masterValue) {
        throw new SparkException(
          s"Master had $config=$masterValue but executor had $config=$executorValue")
      }
    }
    sc.stop()
  }
}

object UserClasspathFirstTest {
  def main(args: Array[String]): Unit = {
    val ccl = Thread.currentThread().getContextClassLoader()
    val resource = ccl.getResourceAsStream("test.resource")
    val bytes = ByteStreams.toByteArray(resource)
    val contents = new String(bytes, 0, bytes.length, StandardCharsets.UTF_8)
    if (contents != "USER") {
      throw new SparkException("Should have read user resource, but instead read: " + contents)
    }
  }
}

class TestFileSystem extends org.apache.hadoop.fs.LocalFileSystem {
  private def local(path: Path): Path = {
    // Ignore the scheme for testing.
    new Path(path.toUri.getPath)
  }

  private def toRemote(status: FileStatus): FileStatus = {
    val path = s"s3a://${status.getPath.toUri.getPath}"
    status.setPath(new Path(path))
    status
  }

  override def getFileStatus(path: Path): FileStatus = super.getFileStatus(local(path))

  override def globStatus(pathPattern: Path): Array[FileStatus] = {
    val newPath = new Path(pathPattern.toUri.getPath)
    super.globStatus(newPath).map(toRemote)
  }

  override def listStatus(path: Path): Array[FileStatus] = {
    super.listStatus(local(path)).map(toRemote)
  }

  override def copyToLocalFile(src: Path, dst: Path): Unit = {
    super.copyToLocalFile(local(src), dst)
  }

  override def open(path: Path): FSDataInputStream = super.open(local(path))
}

class TestSparkApplication extends SparkApplication with Matchers {

  override def start(args: Array[String], conf: SparkConf): Unit = {
    assert(args.length === 1)
    assert(args(0) === "hello")
    assert(conf.get("spark.test.hello") === "world")
    assert(sys.props.get("spark.test.hello") === None)

    // This is how the test verifies the application was actually run.
    throw new SparkException(args(0))
  }

}
