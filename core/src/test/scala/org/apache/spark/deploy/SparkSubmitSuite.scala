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

import java.io.{File, OutputStream, PrintStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkException, TestUtils}
import org.apache.spark.deploy.SparkSubmit._
import org.apache.spark.util.Utils
import org.scalatest.FunSuite
import org.scalatest.Matchers

class SparkSubmitSuite extends FunSuite with Matchers {
  def beforeAll() {
    System.setProperty("spark.testing", "true")
  }

  val noOpOutputStream = new OutputStream {
    def write(b: Int) = {}
  }

  /** Simple PrintStream that reads data into a buffer */
  class BufferPrintStream extends PrintStream(noOpOutputStream) {
    var lineBuffer = ArrayBuffer[String]()
    override def println(line: String) {
      lineBuffer += line
    }
  }

  /** Returns true if the script exits and the given search string is printed. */
  def testPrematureExit(input: Array[String], searchString: String) = {
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
    val (childArgs, classpath, sysProps, mainClass) = createLaunchEnv(appArgs)
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
    val (childArgs, classpath, sysProps, mainClass) = createLaunchEnv(appArgs)
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
    val (childArgs, classpath, sysProps, mainClass) = createLaunchEnv(appArgs)
    val childArgsStr = childArgs.mkString(" ")
    childArgsStr should startWith ("--memory 4g --cores 5 --supervise")
    childArgsStr should include regex ("launch spark://h:p .*thejar.jar org.SomeClass arg1 arg2")
    mainClass should be ("org.apache.spark.deploy.Client")
    classpath should have size (0)
    sysProps should have size (5)
    sysProps.keys should contain ("SPARK_SUBMIT")
    sysProps.keys should contain ("spark.master")
    sysProps.keys should contain ("spark.app.name")
    sysProps.keys should contain ("spark.jars")
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
    val (childArgs, classpath, sysProps, mainClass) = createLaunchEnv(appArgs)
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
    val (childArgs, classpath, sysProps, mainClass) = createLaunchEnv(appArgs)
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
    val (_, _, sysProps, mainClass) = createLaunchEnv(appArgs)
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

  test("spark submit includes jars passed in through --jar") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val jar1 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassA"))
    val jar2 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassB"))
    val jarsString = Seq(jar1, jar2).map(j => j.toString).mkString(",")
    val args = Seq(
      "--class", JarCreationTest.getClass.getName.stripSuffix("$"),
      "--name", "testApp",
      "--master", "local-cluster[2,1,512]",
      "--jars", jarsString,
      unusedJar.toString)
    runSparkSubmit(args)
  }

  // NOTE: This is an expensive operation in terms of time (10 seconds+). Use sparingly.
  def runSparkSubmit(args: Seq[String]): String = {
    val sparkHome = sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    Utils.executeAndGetOutput(
      Seq("./bin/spark-submit") ++ args,
      new File(sparkHome),
      Map("SPARK_TESTING" -> "1", "SPARK_HOME" -> sparkHome))
  }
}

object JarCreationTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val result = sc.makeRDD(1 to 100, 10).mapPartitions { x =>
      var foundClasses = false
      try {
        Class.forName("SparkSubmitClassA", true, Thread.currentThread().getContextClassLoader)
        Class.forName("SparkSubmitClassA", true, Thread.currentThread().getContextClassLoader)
        foundClasses = true
      } catch {
        case _: Throwable => // catch all
      }
      Seq(foundClasses).iterator
    }.collect()
    if (result.contains(false)) {
      throw new Exception("Could not load user defined classes inside of executors")
    }
  }
}

object SimpleApplicationTest {
  def main(args: Array[String]) {
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
