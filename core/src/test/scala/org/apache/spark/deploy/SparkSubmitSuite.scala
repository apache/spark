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

import scala.collection.mutable.ArrayBuffer

import com.google.common.base.Charsets.UTF_8
import com.google.common.io.ByteStreams
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.deploy.SparkSubmit._
import org.apache.spark.util.{ResetSystemProperties, Utils}

// Note: this suite mixes in ResetSystemProperties because SparkSubmit.main() sets a bunch
// of properties that neeed to be cleared after tests.
class SparkSubmitSuite extends FunSuite with Matchers with ResetSystemProperties with Timeouts {
  def beforeAll() {
    System.setProperty("spark.testing", "true")
  }

  private val noOpOutputStream = new OutputStream {
    def write(b: Int) = {}
  }

  /** Simple PrintStream that reads data into a buffer */
  private class BufferPrintStream extends PrintStream(noOpOutputStream) {
    var lineBuffer = ArrayBuffer[String]()
    override def println(line: String) {
      lineBuffer += line
    }
  }

  /** Returns true if the script exits and the given search string is printed. */
  private def testPrematureExit(input: Array[String], searchString: String) = {
    val printStream = new BufferPrintStream()
    SparkSubmit.printStream = printStream

    @volatile var exitedCleanly = false
    SparkSubmit.exitFn = () => exitedCleanly = true

    val thread = new Thread {
      override def run() = try {
        SparkSubmit.main(input)
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

  test("prints usage on empty input") {
    testPrematureExit(Array[String](), "Usage: spark-submit")
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
      "--conf", "spark.shuffle.spill=false",
      "thejar.jar",
      "arg1", "arg2")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (childArgs, classpath, sysProps, mainClass) = prepareSubmitEnvironment(appArgs)
    val childArgsStr = childArgs.mkString(" ")
    childArgsStr should include ("--class org.SomeClass")
    childArgsStr should include ("--executor-memory 5g")
    childArgsStr should include ("--driver-memory 4g")
    childArgsStr should include ("--executor-cores 5")
    childArgsStr should include ("--arg arg1 --arg arg2")
    childArgsStr should include ("--queue thequeue")
    childArgsStr should include ("--num-executors 6")
    childArgsStr should include regex ("--jar .*thejar.jar")
    childArgsStr should include regex ("--addJars .*one.jar,.*two.jar,.*three.jar")
    childArgsStr should include regex ("--files .*file1.txt,.*file2.txt")
    childArgsStr should include regex ("--archives .*archive1.txt,.*archive2.txt")
    mainClass should be ("org.apache.spark.deploy.yarn.Client")
    classpath should have length (0)
    sysProps("spark.app.name") should be ("beauty")
    sysProps("spark.shuffle.spill") should be ("false")
    sysProps("SPARK_SUBMIT") should be ("true")
    sysProps.keys should not contain ("spark.jars")
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
      "--conf", "spark.shuffle.spill=false",
      "thejar.jar",
      "arg1", "arg2")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (childArgs, classpath, sysProps, mainClass) = prepareSubmitEnvironment(appArgs)
    childArgs.mkString(" ") should be ("arg1 arg2")
    mainClass should be ("org.SomeClass")
    classpath should have length (4)
    classpath(0) should endWith ("thejar.jar")
    classpath(1) should endWith ("one.jar")
    classpath(2) should endWith ("two.jar")
    classpath(3) should endWith ("three.jar")
    sysProps("spark.app.name") should be ("trill")
    sysProps("spark.executor.memory") should be ("5g")
    sysProps("spark.executor.cores") should be ("5")
    sysProps("spark.yarn.queue") should be ("thequeue")
    sysProps("spark.executor.instances") should be ("6")
    sysProps("spark.yarn.dist.files") should include regex (".*file1.txt,.*file2.txt")
    sysProps("spark.yarn.dist.archives") should include regex (".*archive1.txt,.*archive2.txt")
    sysProps("spark.jars") should include regex (".*one.jar,.*two.jar,.*three.jar,.*thejar.jar")
    sysProps("SPARK_SUBMIT") should be ("true")
    sysProps("spark.shuffle.spill") should be ("false")
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
      "--conf", "spark.shuffle.spill=false",
      "thejar.jar",
      "arg1", "arg2")
    val appArgs = new SparkSubmitArguments(clArgs)
    appArgs.useRest = useRest
    val (childArgs, classpath, sysProps, mainClass) = prepareSubmitEnvironment(appArgs)
    val childArgsStr = childArgs.mkString(" ")
    if (useRest) {
      childArgsStr should endWith ("thejar.jar org.SomeClass arg1 arg2")
      mainClass should be ("org.apache.spark.deploy.rest.StandaloneRestClient")
    } else {
      childArgsStr should startWith ("--supervise --memory 4g --cores 5")
      childArgsStr should include regex "launch spark://h:p .*thejar.jar org.SomeClass arg1 arg2"
      mainClass should be ("org.apache.spark.deploy.Client")
    }
    classpath should have size 0
    sysProps should have size 8
    sysProps.keys should contain ("SPARK_SUBMIT")
    sysProps.keys should contain ("spark.master")
    sysProps.keys should contain ("spark.app.name")
    sysProps.keys should contain ("spark.jars")
    sysProps.keys should contain ("spark.driver.memory")
    sysProps.keys should contain ("spark.driver.cores")
    sysProps.keys should contain ("spark.driver.supervise")
    sysProps.keys should contain ("spark.shuffle.spill")
    sysProps("spark.shuffle.spill") should be ("false")
  }

  test("handles standalone client mode") {
    val clArgs = Seq(
      "--deploy-mode", "client",
      "--master", "spark://h:p",
      "--executor-memory", "5g",
      "--total-executor-cores", "5",
      "--class", "org.SomeClass",
      "--driver-memory", "4g",
      "--conf", "spark.shuffle.spill=false",
      "thejar.jar",
      "arg1", "arg2")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (childArgs, classpath, sysProps, mainClass) = prepareSubmitEnvironment(appArgs)
    childArgs.mkString(" ") should be ("arg1 arg2")
    mainClass should be ("org.SomeClass")
    classpath should have length (1)
    classpath(0) should endWith ("thejar.jar")
    sysProps("spark.executor.memory") should be ("5g")
    sysProps("spark.cores.max") should be ("5")
    sysProps("spark.shuffle.spill") should be ("false")
  }

  test("handles mesos client mode") {
    val clArgs = Seq(
      "--deploy-mode", "client",
      "--master", "mesos://h:p",
      "--executor-memory", "5g",
      "--total-executor-cores", "5",
      "--class", "org.SomeClass",
      "--driver-memory", "4g",
      "--conf", "spark.shuffle.spill=false",
      "thejar.jar",
      "arg1", "arg2")
    val appArgs = new SparkSubmitArguments(clArgs)
    val (childArgs, classpath, sysProps, mainClass) = prepareSubmitEnvironment(appArgs)
    childArgs.mkString(" ") should be ("arg1 arg2")
    mainClass should be ("org.SomeClass")
    classpath should have length (1)
    classpath(0) should endWith ("thejar.jar")
    sysProps("spark.executor.memory") should be ("5g")
    sysProps("spark.cores.max") should be ("5")
    sysProps("spark.shuffle.spill") should be ("false")
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
    val (_, _, sysProps, mainClass) = prepareSubmitEnvironment(appArgs)
    sysProps("spark.executor.memory") should be ("5g")
    sysProps("spark.master") should be ("yarn-cluster")
    mainClass should be ("org.apache.spark.deploy.yarn.Client")
  }

  test("launch simple application with spark-submit") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val args = Seq(
      "--class", SimpleApplicationTest.getClass.getName.stripSuffix("$"),
      "--name", "testApp",
      "--master", "local",
      unusedJar.toString)
    runSparkSubmit(args)
  }

  test("includes jars passed in through --jars") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val jar1 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassA"))
    val jar2 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassB"))
    val jarsString = Seq(jar1, jar2).map(j => j.toString).mkString(",")
    val args = Seq(
      "--class", JarCreationTest.getClass.getName.stripSuffix("$"),
      "--name", "testApp",
      "--master", "local-cluster[2,1,512]",
      "--jars", jarsString,
      unusedJar.toString, "SparkSubmitClassA", "SparkSubmitClassB")
    runSparkSubmit(args)
  }

  test("includes jars passed in through --packages") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val packagesString = "com.databricks:spark-csv_2.10:0.1,com.databricks:spark-avro_2.10:0.1"
    val args = Seq(
      "--class", JarCreationTest.getClass.getName.stripSuffix("$"),
      "--name", "testApp",
      "--master", "local-cluster[2,1,512]",
      "--packages", packagesString,
      "--conf", "spark.ui.enabled=false",
      unusedJar.toString,
      "com.databricks.spark.csv.DefaultSource", "com.databricks.spark.avro.DefaultSource")
    runSparkSubmit(args)
  }

  test("resolves command line argument paths correctly") {
    val jars = "/jar1,/jar2"                 // --jars
    val files = "hdfs:/file1,file2"          // --files
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
    val sysProps = SparkSubmit.prepareSubmitEnvironment(appArgs)._3
    appArgs.jars should be (Utils.resolveURIs(jars))
    appArgs.files should be (Utils.resolveURIs(files))
    sysProps("spark.jars") should be (Utils.resolveURIs(jars + ",thejar.jar"))
    sysProps("spark.files") should be (Utils.resolveURIs(files))

    // Test files and archives (Yarn)
    val clArgs2 = Seq(
      "--master", "yarn-client",
      "--class", "org.SomeClass",
      "--files", files,
      "--archives", archives,
      "thejar.jar"
    )
    val appArgs2 = new SparkSubmitArguments(clArgs2)
    val sysProps2 = SparkSubmit.prepareSubmitEnvironment(appArgs2)._3
    appArgs2.files should be (Utils.resolveURIs(files))
    appArgs2.archives should be (Utils.resolveURIs(archives))
    sysProps2("spark.yarn.dist.files") should be (Utils.resolveURIs(files))
    sysProps2("spark.yarn.dist.archives") should be (Utils.resolveURIs(archives))

    // Test python files
    val clArgs3 = Seq(
      "--master", "local",
      "--py-files", pyFiles,
      "mister.py"
    )
    val appArgs3 = new SparkSubmitArguments(clArgs3)
    val sysProps3 = SparkSubmit.prepareSubmitEnvironment(appArgs3)._3
    appArgs3.pyFiles should be (Utils.resolveURIs(pyFiles))
    sysProps3("spark.submit.pyFiles") should be (
      PythonRunner.formatPaths(Utils.resolveURIs(pyFiles)).mkString(","))
  }

  test("resolves config paths correctly") {
    val jars = "/jar1,/jar2" // spark.jars
    val files = "hdfs:/file1,file2" // spark.files / spark.yarn.dist.files
    val archives = "file:/archive1,archive2" // spark.yarn.dist.archives
    val pyFiles = "py-file1,py-file2" // spark.submit.pyFiles

    // Test jars and files
    val f1 = File.createTempFile("test-submit-jars-files", "")
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
    val sysProps = SparkSubmit.prepareSubmitEnvironment(appArgs)._3
    sysProps("spark.jars") should be(Utils.resolveURIs(jars + ",thejar.jar"))
    sysProps("spark.files") should be(Utils.resolveURIs(files))

    // Test files and archives (Yarn)
    val f2 = File.createTempFile("test-submit-files-archives", "")
    val writer2 = new PrintWriter(f2)
    writer2.println("spark.yarn.dist.files " + files)
    writer2.println("spark.yarn.dist.archives " + archives)
    writer2.close()
    val clArgs2 = Seq(
      "--master", "yarn-client",
      "--class", "org.SomeClass",
      "--properties-file", f2.getPath,
      "thejar.jar"
    )
    val appArgs2 = new SparkSubmitArguments(clArgs2)
    val sysProps2 = SparkSubmit.prepareSubmitEnvironment(appArgs2)._3
    sysProps2("spark.yarn.dist.files") should be(Utils.resolveURIs(files))
    sysProps2("spark.yarn.dist.archives") should be(Utils.resolveURIs(archives))

    // Test python files
    val f3 = File.createTempFile("test-submit-python-files", "")
    val writer3 = new PrintWriter(f3)
    writer3.println("spark.submit.pyFiles " + pyFiles)
    writer3.close()
    val clArgs3 = Seq(
      "--master", "local",
      "--properties-file", f3.getPath,
      "mister.py"
    )
    val appArgs3 = new SparkSubmitArguments(clArgs3)
    val sysProps3 = SparkSubmit.prepareSubmitEnvironment(appArgs3)._3
    sysProps3("spark.submit.pyFiles") should be(
      PythonRunner.formatPaths(Utils.resolveURIs(pyFiles)).mkString(","))
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

  // NOTE: This is an expensive operation in terms of time (10 seconds+). Use sparingly.
  private def runSparkSubmit(args: Seq[String]): Unit = {
    val sparkHome = sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    val process = Utils.executeCommand(
      Seq("./bin/spark-submit") ++ args,
      new File(sparkHome),
      Map("SPARK_TESTING" -> "1", "SPARK_HOME" -> sparkHome))
    failAfter(60 seconds) { process.waitFor() }
    // Ensure we still kill the process in case it timed out
    process.destroy()
  }

  private def forConfDir(defaults: Map[String, String]) (f: String => Unit) = {
    val tmpDir = Utils.createTempDir()

    val defaultsConf = new File(tmpDir.getAbsolutePath, "spark-defaults.conf")
    val writer = new OutputStreamWriter(new FileOutputStream(defaultsConf))
    for ((key, value) <- defaults) writer.write(s"$key $value\n")

    writer.close()

    try {
      f(tmpDir.getAbsolutePath)
    } finally {
      Utils.deleteRecursively(tmpDir)
    }
  }
}

object JarCreationTest extends Logging {
  def main(args: Array[String]) {
    Utils.configTestLog4j("INFO")
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val result = sc.makeRDD(1 to 100, 10).mapPartitions { x =>
      var exception: String = null
      try {
        Class.forName(args(0), true, Thread.currentThread().getContextClassLoader)
        Class.forName(args(1), true, Thread.currentThread().getContextClassLoader)
      } catch {
        case t: Throwable =>
          exception = t + "\n" + t.getStackTraceString
          exception = exception.replaceAll("\n", "\n\t")
      }
      Option(exception).toSeq.iterator
    }.collect()
    if (result.nonEmpty) {
      throw new Exception("Could not load user class from jar:\n" + result(0))
    }
  }
}

object SimpleApplicationTest {
  def main(args: Array[String]) {
    Utils.configTestLog4j("INFO")
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
  }
}

object UserClasspathFirstTest {
  def main(args: Array[String]) {
    val ccl = Thread.currentThread().getContextClassLoader()
    val resource = ccl.getResourceAsStream("test.resource")
    val bytes = ByteStreams.toByteArray(resource)
    val contents = new String(bytes, 0, bytes.length, UTF_8)
    if (contents != "USER") {
      throw new SparkException("Should have read user resource, but instead read: " + contents)
    }
  }
}
