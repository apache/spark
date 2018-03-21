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
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import com.google.common.io.ByteStreams
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FSDataInputStream, Path}
import org.scalatest.{BeforeAndAfterEach, Matchers}
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.TestUtils
import org.apache.spark.TestUtils.JavaSourceFromString
import org.apache.spark.api.r.RUtils
import org.apache.spark.deploy.SparkSubmit._
import org.apache.spark.deploy.SparkSubmitUtils.MavenCoordinate
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.EventLoggingListener
import org.apache.spark.util.{CommandLineUtils, ResetSystemProperties, Utils}

trait TestPrematureExit {
  suite: SparkFunSuite =>

  private val noOpOutputStream = new OutputStream {
    def write(b: Int) = {}
  }

  /** Simple PrintStream that reads data into a buffer */
  private class BufferPrintStream extends PrintStream(noOpOutputStream) {
    var lineBuffer = ArrayBuffer[String]()
    // scalastyle:off println
    override def println(line: String) {
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
    mainObject.exitFn = (_) => exitedCleanly = true

    val thread = new Thread {
      override def run() = try {
        mainObject.main(input)
      } catch {
        // If exceptions occur after the "exit" has happened, fine to ignore them.
        // These represent code paths not reachable during normal execution.
        case e: Exception => if (!exitedCleanly) throw e
      }
    }
    thread.start()
    thread.join()
    val joined = printStream.lineBuffer.mkString("\n")
    if (!joined.contains(searchString)) {
      fail(s"Search string '$searchString' not found in $joined")
    }
  }
}

// Note: this suite mixes in ResetSystemProperties because SparkSubmit.main() sets a bunch
// of properties that needed to be cleared after tests.
class SparkSubmitSuite
  extends SparkFunSuite
  with Matchers
  with BeforeAndAfterEach
  with ResetSystemProperties
  with TimeLimits
  with TestPrematureExit {

  import SparkSubmitSuite._

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  override def beforeEach() {
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
    testPrematureExit(Array("foo.jar"), "No main class")
  }

  test("handles arguments with --key=val") {
    val clArgs = Seq(
      "--jars=one.jar,two.jar,three.jar",
      "--name=myApp")
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

  test("specify deploy mode through configuration") {
    val clArgs = Seq(
      "--master", "yarn",
      "--conf", "spark.submit.deployMode=client",
      "--class", "org.SomeClass",
      "thejar.jar"
    )
    val appArgs = new SparkSubmitArguments(clArgs)
    val (_, _, conf, _) = prepareSubmitEnvironment(appArgs)

    appArgs.deployMode should be ("client")
    conf.get("spark.submit.deployMode") should be ("client")

    // Both cmd line and configuration are specified, cmdline option takes the priority
    val clArgs1 = Seq(
      "--master", "yarn",
      "--deploy-mode", "cluster",
      "--conf", "spark.submit.deployMode=client",
      "-class", "org.SomeClass",
      "thejar.jar"
    )
    val appArgs1 = new SparkSubmitArguments(clArgs1)
    val (_, _, conf1, _) = prepareSubmitEnvironment(appArgs1)

    appArgs1.deployMode should be ("cluster")
    conf1.get("spark.submit.deployMode") should be ("cluster")

    // Neither cmdline nor configuration are specified, client mode is the default choice
    val clArgs2 = Seq(
      "--master", "yarn",
      "--class", "org.SomeClass",
      "thejar.jar"
    )
    val appArgs2 = new SparkSubmitArguments(clArgs2)
    appArgs2.deployMode should be (null)

    val (_, _, conf2, _) = prepareSubmitEnvironment(appArgs2)
    appArgs2.deployMode should be ("client")
    conf2.get("spark.submit.deployMode") should be ("client")
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
    val (childArgs, classpath, conf, mainClass) = prepareSubmitEnvironment(appArgs)
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
    conf.get("spark.ui.enabled") should be ("false")
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
    val (childArgs, classpath, conf, mainClass) = prepareSubmitEnvironment(appArgs)
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
    conf.get("spark.ui.enabled") should be ("false")
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
    val (childArgs, classpath, conf, mainClass) = prepareSubmitEnvironment(appArgs)
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
    confMap.keys should contain ("spark.jars")
    confMap.keys should contain ("spark.driver.memory")
    confMap.keys should contain ("spark.driver.cores")
    confMap.keys should contain ("spark.driver.supervise")
    confMap.keys should contain ("spark.ui.enabled")
    confMap.keys should contain ("spark.submit.deployMode")
    conf.get("spark.ui.enabled") should be ("false")
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
    val (childArgs, classpath, conf, mainClass) = prepareSubmitEnvironment(appArgs)
    childArgs.mkString(" ") should be ("arg1 arg2")
    mainClass should be ("org.SomeClass")
    classpath should have length (1)
    classpath(0) should endWith ("thejar.jar")
    conf.get("spark.executor.memory") should be ("5g")
    conf.get("spark.cores.max") should be ("5")
    conf.get("spark.ui.enabled") should be ("false")
  }

  test("handles mesos client mode") {
    val clArgs = Seq(
      "--deploy-mode", "client",
      "--master", "mesos://h:p",
      "--executor-memory", "5g",
      "--total-executor-cores", "5",
      "--class", "org.SomeClass",
      "--driver-memory", "4g",
      "--conf", "spark.ui.enabled=false",
      "thejar.jar",
      "arg1", "arg2")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (childArgs, classpath, conf, mainClass) = prepareSubmitEnvironment(appArgs)
    childArgs.mkString(" ") should be ("arg1 arg2")
    mainClass should be ("org.SomeClass")
    classpath should have length (1)
    classpath(0) should endWith ("thejar.jar")
    conf.get("spark.executor.memory") should be ("5g")
    conf.get("spark.cores.max") should be ("5")
    conf.get("spark.ui.enabled") should be ("false")
  }

  test("handles k8s cluster mode") {
    val clArgs = Seq(
      "--deploy-mode", "cluster",
      "--master", "k8s://host:port",
      "--executor-memory", "5g",
      "--class", "org.SomeClass",
      "--driver-memory", "4g",
      "--conf", "spark.kubernetes.namespace=spark",
      "--conf", "spark.kubernetes.driver.container.image=bar",
      "/home/thejar.jar",
      "arg1")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (childArgs, classpath, conf, mainClass) = prepareSubmitEnvironment(appArgs)

    val childArgsMap = childArgs.grouped(2).map(a => a(0) -> a(1)).toMap
    childArgsMap.get("--primary-java-resource") should be (Some("file:/home/thejar.jar"))
    childArgsMap.get("--main-class") should be (Some("org.SomeClass"))
    childArgsMap.get("--arg") should be (Some("arg1"))
    mainClass should be (KUBERNETES_CLUSTER_SUBMIT_CLASS)
    classpath should have length (0)
    conf.get("spark.master") should be ("k8s://https://host:port")
    conf.get("spark.executor.memory") should be ("5g")
    conf.get("spark.driver.memory") should be ("4g")
    conf.get("spark.kubernetes.namespace") should be ("spark")
    conf.get("spark.kubernetes.driver.container.image") should be ("bar")
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
    val (_, _, conf, mainClass) = prepareSubmitEnvironment(appArgs)
    conf.get("spark.executor.memory") should be ("5g")
    conf.get("spark.master") should be ("yarn")
    conf.get("spark.submit.deployMode") should be ("cluster")
    mainClass should be (SparkSubmit.YARN_CLUSTER_SUBMIT_CLASS)
  }

  test("SPARK-21568 ConsoleProgressBar should be enabled only in shells") {
    // Unset from system properties since this config is defined in the root pom's test config.
    sys.props -= UI_SHOW_CONSOLE_PROGRESS.key

    val clArgs1 = Seq("--class", "org.apache.spark.repl.Main", "spark-shell")
    val appArgs1 = new SparkSubmitArguments(clArgs1)
    val (_, _, conf1, _) = prepareSubmitEnvironment(appArgs1)
    conf1.get(UI_SHOW_CONSOLE_PROGRESS) should be (true)

    val clArgs2 = Seq("--class", "org.SomeClass", "thejar.jar")
    val appArgs2 = new SparkSubmitArguments(clArgs2)
    val (_, _, conf2, _) = prepareSubmitEnvironment(appArgs2)
    assert(!conf2.contains(UI_SHOW_CONSOLE_PROGRESS))
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
    val testDir = Utils.createTempDir()
    testDir.deleteOnExit()
    val testDirPath = new Path(testDir.getAbsolutePath())
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val fileSystem = Utils.getHadoopFileSystem("/",
      SparkHadoopUtil.get.newConfiguration(new SparkConf()))
    try {
      val args = Seq(
        "--class", SimpleApplicationTest.getClass.getName.stripSuffix("$"),
        "--name", "testApp",
        "--master", "local",
        "--conf", "spark.ui.enabled=false",
        "--conf", "spark.master.rest.enabled=false",
        "--conf", "spark.executorEnv.HADOOP_CREDSTORE_PASSWORD=secret_password",
        "--conf", "spark.eventLog.enabled=true",
        "--conf", "spark.eventLog.testing=true",
        "--conf", s"spark.eventLog.dir=${testDirPath.toUri.toString}",
        "--conf", "spark.hadoop.fs.defaultFS=unsupported://example.com",
        unusedJar.toString)
      runSparkSubmit(args)
      val listStatus = fileSystem.listStatus(testDirPath)
      val logData = EventLoggingListener.openEventLog(listStatus.last.getPath, fileSystem)
      Source.fromInputStream(logData).getLines().foreach { line =>
        assert(!line.contains("secret_password"))
      }
    } finally {
      Utils.deleteRecursively(testDir)
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
        unusedJar.toString,
        "my.great.lib.MyLib", "my.great.dep.MyLib")
      runSparkSubmit(args)
    }
  }

  test("includes jars passed through spark.jars.packages and spark.jars.repositories") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val main = MavenCoordinate("my.great.lib", "mylib", "0.1")
    val dep = MavenCoordinate("my.great.dep", "mylib", "0.1")
    // Test using "spark.jars.packages" and "spark.jars.repositories" configurations.
    IvyTestUtils.withRepository(main, Some(dep.toString), None) { repo =>
      val args = Seq(
        "--class", JarCreationTest.getClass.getName.stripSuffix("$"),
        "--name", "testApp",
        "--master", "local-cluster[2,1,1024]",
        "--conf", "spark.jars.packages=my.great.lib:mylib:0.1,my.great.dep:mylib:0.1",
        "--conf", s"spark.jars.repositories=$repo",
        "--conf", "spark.ui.enabled=false",
        "--conf", "spark.master.rest.enabled=false",
        unusedJar.toString,
        "my.great.lib.MyLib", "my.great.dep.MyLib")
      runSparkSubmit(args)
    }
  }

  // TODO(SPARK-9603): Building a package is flaky on Jenkins Maven builds.
  // See https://gist.github.com/shivaram/3a2fecce60768a603dac for a error log
  ignore("correctly builds R packages included in a jar with --packages") {
    assume(RUtils.isRInstalled, "R isn't installed on this machine.")
    // Check if the SparkR package is installed
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
        "--verbose",
        "--conf", "spark.ui.enabled=false",
        rScriptDir)
      runSparkSubmit(args)
    }
  }

  test("include an external JAR in SparkR") {
    assume(RUtils.isRInstalled, "R isn't installed on this machine.")
    val sparkHome = sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    // Check if the SparkR package is installed
    assume(RUtils.isSparkRInstalled, "SparkR is not installed in this build.")
    val rScriptDir =
      Seq(sparkHome, "R", "pkg", "tests", "fulltests", "jarTest.R").mkString(File.separator)
    assert(new File(rScriptDir).exists)

    // compile a small jar containing a class that will be called from R code.
    val tempDir = Utils.createTempDir()
    val srcDir = new File(tempDir, "sparkrtest")
    srcDir.mkdirs()
    val excSource = new JavaSourceFromString(new File(srcDir, "DummyClass").toURI.getPath,
      """package sparkrtest;
        |
        |public class DummyClass implements java.io.Serializable {
        |  public static String helloWorld(String arg) { return "Hello " + arg; }
        |  public static int addStuff(int arg1, int arg2) { return arg1 + arg2; }
        |}
      """.stripMargin)
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

  test("resolves command line argument paths correctly") {
    val jars = "/jar1,/jar2"                 // --jars
    val files = "local:/file1,file2"          // --files
    val archives = "file:/archive1,archive2" // --archives
    val pyFiles = "py-file1,py-file2"        // --py-files

    // Test jars and files
    val clArgs = Seq(
      "--master", "local",
      "--class", "org.SomeClass",
      "--jars", jars,
      "--files", files,
      "thejar.jar")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (_, _, conf, _) = SparkSubmit.prepareSubmitEnvironment(appArgs)
    appArgs.jars should be (Utils.resolveURIs(jars))
    appArgs.files should be (Utils.resolveURIs(files))
    conf.get("spark.jars") should be (Utils.resolveURIs(jars + ",thejar.jar"))
    conf.get("spark.files") should be (Utils.resolveURIs(files))

    // Test files and archives (Yarn)
    val clArgs2 = Seq(
      "--master", "yarn",
      "--class", "org.SomeClass",
      "--files", files,
      "--archives", archives,
      "thejar.jar"
    )
    val appArgs2 = new SparkSubmitArguments(clArgs2)
    val (_, _, conf2, _) = SparkSubmit.prepareSubmitEnvironment(appArgs2)
    appArgs2.files should be (Utils.resolveURIs(files))
    appArgs2.archives should be (Utils.resolveURIs(archives))
    conf2.get("spark.yarn.dist.files") should be (Utils.resolveURIs(files))
    conf2.get("spark.yarn.dist.archives") should be (Utils.resolveURIs(archives))

    // Test python files
    val clArgs3 = Seq(
      "--master", "local",
      "--py-files", pyFiles,
      "--conf", "spark.pyspark.driver.python=python3.4",
      "--conf", "spark.pyspark.python=python3.5",
      "mister.py"
    )
    val appArgs3 = new SparkSubmitArguments(clArgs3)
    val (_, _, conf3, _) = SparkSubmit.prepareSubmitEnvironment(appArgs3)
    appArgs3.pyFiles should be (Utils.resolveURIs(pyFiles))
    conf3.get("spark.submit.pyFiles") should be (
      PythonRunner.formatPaths(Utils.resolveURIs(pyFiles)).mkString(","))
    conf3.get(PYSPARK_DRIVER_PYTHON.key) should be ("python3.4")
    conf3.get(PYSPARK_PYTHON.key) should be ("python3.5")
  }

  test("resolves config paths correctly") {
    val jars = "/jar1,/jar2" // spark.jars
    val files = "local:/file1,file2" // spark.files / spark.yarn.dist.files
    val archives = "file:/archive1,archive2" // spark.yarn.dist.archives
    val pyFiles = "py-file1,py-file2" // spark.submit.pyFiles

    val tmpDir = Utils.createTempDir()

    // Test jars and files
    val f1 = File.createTempFile("test-submit-jars-files", "", tmpDir)
    val writer1 = new PrintWriter(f1)
    writer1.println("spark.jars " + jars)
    writer1.println("spark.files " + files)
    writer1.close()
    val clArgs = Seq(
      "--master", "local",
      "--class", "org.SomeClass",
      "--properties-file", f1.getPath,
      "thejar.jar"
    )
    val appArgs = new SparkSubmitArguments(clArgs)
    val (_, _, conf, _) = SparkSubmit.prepareSubmitEnvironment(appArgs)
    conf.get("spark.jars") should be(Utils.resolveURIs(jars + ",thejar.jar"))
    conf.get("spark.files") should be(Utils.resolveURIs(files))

    // Test files and archives (Yarn)
    val f2 = File.createTempFile("test-submit-files-archives", "", tmpDir)
    val writer2 = new PrintWriter(f2)
    writer2.println("spark.yarn.dist.files " + files)
    writer2.println("spark.yarn.dist.archives " + archives)
    writer2.close()
    val clArgs2 = Seq(
      "--master", "yarn",
      "--class", "org.SomeClass",
      "--properties-file", f2.getPath,
      "thejar.jar"
    )
    val appArgs2 = new SparkSubmitArguments(clArgs2)
    val (_, _, conf2, _) = SparkSubmit.prepareSubmitEnvironment(appArgs2)
    conf2.get("spark.yarn.dist.files") should be(Utils.resolveURIs(files))
    conf2.get("spark.yarn.dist.archives") should be(Utils.resolveURIs(archives))

    // Test python files
    val f3 = File.createTempFile("test-submit-python-files", "", tmpDir)
    val writer3 = new PrintWriter(f3)
    writer3.println("spark.submit.pyFiles " + pyFiles)
    writer3.close()
    val clArgs3 = Seq(
      "--master", "local",
      "--properties-file", f3.getPath,
      "mister.py"
    )
    val appArgs3 = new SparkSubmitArguments(clArgs3)
    val (_, _, conf3, _) = SparkSubmit.prepareSubmitEnvironment(appArgs3)
    conf3.get("spark.submit.pyFiles") should be(
      PythonRunner.formatPaths(Utils.resolveURIs(pyFiles)).mkString(","))

    // Test remote python files
    val f4 = File.createTempFile("test-submit-remote-python-files", "", tmpDir)
    val writer4 = new PrintWriter(f4)
    val remotePyFiles = "hdfs:///tmp/file1.py,hdfs:///tmp/file2.py"
    writer4.println("spark.submit.pyFiles " + remotePyFiles)
    writer4.close()
    val clArgs4 = Seq(
      "--master", "yarn",
      "--deploy-mode", "cluster",
      "--properties-file", f4.getPath,
      "hdfs:///tmp/mister.py"
    )
    val appArgs4 = new SparkSubmitArguments(clArgs4)
    val (_, _, conf4, _) = SparkSubmit.prepareSubmitEnvironment(appArgs4)
    // Should not format python path for yarn cluster mode
    conf4.get("spark.submit.pyFiles") should be(Utils.resolveURIs(remotePyFiles))
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
    forConfDir(Map("spark.executor.memory" -> "2.3g")) { path =>
      val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
      val args = Seq(
        "--class", SimpleApplicationTest.getClass.getName.stripSuffix("$"),
        "--name", "testApp",
        "--master", "local",
        unusedJar.toString)
      val appArgs = new SparkSubmitArguments(args, Map("SPARK_CONF_DIR" -> path))
      assert(appArgs.propertiesFile != null)
      assert(appArgs.propertiesFile.startsWith(path))
      appArgs.executorMemory should be ("2.3g")
    }
  }

  test("support glob path") {
    val tmpJarDir = Utils.createTempDir()
    val jar1 = TestUtils.createJarWithFiles(Map("test.resource" -> "1"), tmpJarDir)
    val jar2 = TestUtils.createJarWithFiles(Map("test.resource" -> "USER"), tmpJarDir)

    val tmpFileDir = Utils.createTempDir()
    val file1 = File.createTempFile("tmpFile1", "", tmpFileDir)
    val file2 = File.createTempFile("tmpFile2", "", tmpFileDir)

    val tmpPyFileDir = Utils.createTempDir()
    val pyFile1 = File.createTempFile("tmpPy1", ".py", tmpPyFileDir)
    val pyFile2 = File.createTempFile("tmpPy2", ".egg", tmpPyFileDir)

    val tmpArchiveDir = Utils.createTempDir()
    val archive1 = File.createTempFile("archive1", ".zip", tmpArchiveDir)
    val archive2 = File.createTempFile("archive2", ".zip", tmpArchiveDir)

    val args = Seq(
      "--class", UserClasspathFirstTest.getClass.getName.stripPrefix("$"),
      "--name", "testApp",
      "--master", "yarn",
      "--deploy-mode", "client",
      "--jars", s"${tmpJarDir.getAbsolutePath}/*.jar",
      "--files", s"${tmpFileDir.getAbsolutePath}/tmpFile*",
      "--py-files", s"${tmpPyFileDir.getAbsolutePath}/tmpPy*",
      "--archives", s"${tmpArchiveDir.getAbsolutePath}/*.zip",
      jar2.toString)

    val appArgs = new SparkSubmitArguments(args)
    val (_, _, conf, _) = SparkSubmit.prepareSubmitEnvironment(appArgs)
    conf.get("spark.yarn.dist.jars").split(",").toSet should be
      (Set(jar1.toURI.toString, jar2.toURI.toString))
    conf.get("spark.yarn.dist.files").split(",").toSet should be
      (Set(file1.toURI.toString, file2.toURI.toString))
    conf.get("spark.yarn.dist.pyFiles").split(",").toSet should be
      (Set(pyFile1.getAbsolutePath, pyFile2.getAbsolutePath))
    conf.get("spark.yarn.dist.archives").split(",").toSet should be
      (Set(archive1.toURI.toString, archive2.toURI.toString))
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
    assert(FileUtils.readFileToString(new File(outputUri.getPath)) ===
      FileUtils.readFileToString(new File(sourceUri.getPath)))
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
        "abc:/my/file", Utils.createTempDir(), sparkConf, new Configuration(),
        new SecurityManager(sparkConf))
    }
  }

  test("downloadFile - file doesn't exist") {
    val sparkConf = new SparkConf(false)
    val hadoopConf = new Configuration()
    val tmpDir = Utils.createTempDir()
    updateConfWithFakeS3Fs(hadoopConf)
    intercept[FileNotFoundException] {
      DependencyUtils.downloadFile("s3a:/no/such/file", tmpDir, sparkConf, hadoopConf,
        new SecurityManager(sparkConf))
    }
  }

  test("downloadFile does not download local file") {
    val sparkConf = new SparkConf(false)
    val secMgr = new SecurityManager(sparkConf)
    // empty path is considered as local file.
    val tmpDir = Files.createTempDirectory("tmp").toFile
    assert(DependencyUtils.downloadFile("", tmpDir, sparkConf, new Configuration(), secMgr) === "")
    assert(DependencyUtils.downloadFile("/local/file", tmpDir, sparkConf, new Configuration(),
      secMgr) === "/local/file")
  }

  test("download one file to local") {
    val sparkConf = new SparkConf(false)
    val jarFile = File.createTempFile("test", ".jar")
    jarFile.deleteOnExit()
    val content = "hello, world"
    FileUtils.write(jarFile, content)
    val hadoopConf = new Configuration()
    val tmpDir = Files.createTempDirectory("tmp").toFile
    updateConfWithFakeS3Fs(hadoopConf)
    val sourcePath = s"s3a://${jarFile.toURI.getPath}"
    val outputPath = DependencyUtils.downloadFile(sourcePath, tmpDir, sparkConf, hadoopConf,
      new SecurityManager(sparkConf))
    checkDownloadedFile(sourcePath, outputPath)
    deleteTempOutputFile(outputPath)
  }

  test("download list of files to local") {
    val sparkConf = new SparkConf(false)
    val jarFile = File.createTempFile("test", ".jar")
    jarFile.deleteOnExit()
    val content = "hello, world"
    FileUtils.write(jarFile, content)
    val hadoopConf = new Configuration()
    val tmpDir = Files.createTempDirectory("tmp").toFile
    updateConfWithFakeS3Fs(hadoopConf)
    val sourcePaths = Seq("/local/file", s"s3a://${jarFile.toURI.getPath}")
    val outputPaths = DependencyUtils
      .downloadFileList(sourcePaths.mkString(","), tmpDir, sparkConf, hadoopConf,
        new SecurityManager(sparkConf))
      .split(",")

    assert(outputPaths.length === sourcePaths.length)
    sourcePaths.zip(outputPaths).foreach { case (sourcePath, outputPath) =>
      checkDownloadedFile(sourcePath, outputPath)
      deleteTempOutputFile(outputPath)
    }
  }

  test("Avoid re-upload remote resources in yarn client mode") {
    val hadoopConf = new Configuration()
    updateConfWithFakeS3Fs(hadoopConf)

    val tmpDir = Utils.createTempDir()
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
    val (_, _, conf, _) = SparkSubmit.prepareSubmitEnvironment(appArgs, Some(hadoopConf))

    // All the resources should still be remote paths, so that YARN client will not upload again.
    conf.get("spark.yarn.dist.jars") should be (tmpJarPath)
    conf.get("spark.yarn.dist.files") should be (s"s3a://${file.getAbsolutePath}")
    conf.get("spark.yarn.dist.pyFiles") should be (s"s3a://${pyFile.getAbsolutePath}")

    // Local repl jars should be a local path.
    conf.get("spark.repl.local.jars") should (startWith("file:"))

    // local py files should not be a URI format.
    conf.get("spark.submit.pyFiles") should (startWith("/"))
  }

  test("download remote resource if it is not supported by yarn service") {
    testRemoteResources(isHttpSchemeBlacklisted = false, supportMockHttpFs = false)
  }

  test("avoid downloading remote resource if it is supported by yarn service") {
    testRemoteResources(isHttpSchemeBlacklisted = false, supportMockHttpFs = true)
  }

  test("force download from blacklisted schemes") {
    testRemoteResources(isHttpSchemeBlacklisted = true, supportMockHttpFs = true)
  }

  private def testRemoteResources(isHttpSchemeBlacklisted: Boolean,
      supportMockHttpFs: Boolean): Unit = {
    val hadoopConf = new Configuration()
    updateConfWithFakeS3Fs(hadoopConf)
    if (supportMockHttpFs) {
      hadoopConf.set("fs.http.impl", classOf[TestFileSystem].getCanonicalName)
      hadoopConf.set("fs.http.impl.disable.cache", "true")
    }

    val tmpDir = Utils.createTempDir()
    val mainResource = File.createTempFile("tmpPy", ".py", tmpDir)
    val tmpS3Jar = TestUtils.createJarWithFiles(Map("test.resource" -> "USER"), tmpDir)
    val tmpS3JarPath = s"s3a://${new File(tmpS3Jar.toURI).getAbsolutePath}"
    val tmpHttpJar = TestUtils.createJarWithFiles(Map("test.resource" -> "USER"), tmpDir)
    val tmpHttpJarPath = s"http://${new File(tmpHttpJar.toURI).getAbsolutePath}"

    val args = Seq(
      "--class", UserClasspathFirstTest.getClass.getName.stripPrefix("$"),
      "--name", "testApp",
      "--master", "yarn",
      "--deploy-mode", "client",
      "--jars", s"$tmpS3JarPath,$tmpHttpJarPath",
      s"s3a://$mainResource"
    ) ++ (
      if (isHttpSchemeBlacklisted) {
        Seq("--conf", "spark.yarn.dist.forceDownloadSchemes=http,https")
      } else {
        Nil
      }
    )

    val appArgs = new SparkSubmitArguments(args)
    val (_, _, conf, _) = SparkSubmit.prepareSubmitEnvironment(appArgs, Some(hadoopConf))

    val jars = conf.get("spark.yarn.dist.jars").split(",").toSet

    // The URI of remote S3 resource should still be remote.
    assert(jars.contains(tmpS3JarPath))

    if (supportMockHttpFs) {
      // If Http FS is supported by yarn service, the URI of remote http resource should
      // still be remote.
      assert(jars.contains(tmpHttpJarPath))
    } else {
      // If Http FS is not supported by yarn service, or http scheme is configured to be force
      // downloading, the URI of remote http resource should be changed to a local one.
      val jarName = new File(tmpHttpJar.toURI).getName
      val localHttpJar = jars.filter(_.contains(jarName))
      localHttpJar.size should be(1)
      localHttpJar.head should startWith("file:")
    }
  }

  private def forConfDir(defaults: Map[String, String]) (f: String => Unit) = {
    val tmpDir = Utils.createTempDir()

    val defaultsConf = new File(tmpDir.getAbsolutePath, "spark-defaults.conf")
    val writer = new OutputStreamWriter(new FileOutputStream(defaultsConf), StandardCharsets.UTF_8)
    for ((key, value) <- defaults) writer.write(s"$key $value\n")

    writer.close()

    try {
      f(tmpDir.getAbsolutePath)
    } finally {
      Utils.deleteRecursively(tmpDir)
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
      SparkSubmit.main(args)
    }

    assert(exception.getMessage() === "hello")
  }

}

object SparkSubmitSuite extends SparkFunSuite with TimeLimits {

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  // NOTE: This is an expensive operation in terms of time (10 seconds+). Use sparingly.
  def runSparkSubmit(args: Seq[String], root: String = ".."): Unit = {
    val sparkHome = sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    val sparkSubmitFile = if (Utils.isWindows) {
      new File(s"$root\\bin\\spark-submit.cmd")
    } else {
      new File(s"$root/bin/spark-submit")
    }
    val process = Utils.executeCommand(
      Seq(sparkSubmitFile.getCanonicalPath) ++ args,
      new File(sparkHome),
      Map("SPARK_TESTING" -> "1", "SPARK_HOME" -> sparkHome))

    try {
      val exitCode = failAfter(60 seconds) { process.waitFor() }
      if (exitCode != 0) {
        fail(s"Process returned with exit code $exitCode. See the log4j logs for more detail.")
      }
    } finally {
      // Ensure we still kill the process in case it timed out
      process.destroy()
    }
  }
}

object JarCreationTest extends Logging {
  def main(args: Array[String]) {
    TestUtils.configTestLog4j("INFO")
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val result = sc.makeRDD(1 to 100, 10).mapPartitions { x =>
      var exception: String = null
      try {
        Utils.classForName(args(0))
        Utils.classForName(args(1))
      } catch {
        case t: Throwable =>
          exception = t + "\n" + Utils.exceptionString(t)
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
  def main(args: Array[String]) {
    TestUtils.configTestLog4j("INFO")
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
      if (executorValues.size != 1) {
        throw new SparkException(s"Inconsistent values for $config: $executorValues")
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
  def main(args: Array[String]) {
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

  override def isFile(path: Path): Boolean = super.isFile(local(path))

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
    assert(args.size === 1)
    assert(args(0) === "hello")
    assert(conf.get("spark.test.hello") === "world")
    assert(sys.props.get("spark.test.hello") === None)

    // This is how the test verifies the application was actually run.
    throw new SparkException(args(0))
  }

}
