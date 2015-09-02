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

package org.apache.spark.sql.hive

import java.io.File
import java.sql.Timestamp
import java.util.Date

import scala.collection.mutable.ArrayBuffer

import org.scalatest.Matchers
import org.scalatest.concurrent.Timeouts
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.{TestHive, TestHiveContext}
import org.apache.spark.sql.test.ProcessTestUtils.ProcessOutputCapturer
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.util.{ResetSystemProperties, Utils}

/**
 * This suite tests spark-submit with applications using HiveContext.
 */
class HiveSparkSubmitSuite
  extends SparkFunSuite
  with Matchers
  // This test suite sometimes gets extremely slow out of unknown reason on Jenkins.  Here we
  // add a timestamp to provide more diagnosis information.
  with ResetSystemProperties
  with Timeouts {

  // TODO: rewrite these or mark them as slow tests to be run sparingly

  def beforeAll() {
    System.setProperty("spark.testing", "true")
  }

  test("SPARK-8368: includes jars passed in through --jars") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val jar1 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassA"))
    val jar2 = TestUtils.createJarWithClasses(Seq("SparkSubmitClassB"))
    val jar3 = TestHive.getHiveFile("hive-contrib-0.13.1.jar").getCanonicalPath
    val jar4 = TestHive.getHiveFile("hive-hcatalog-core-0.13.1.jar").getCanonicalPath
    val jarsString = Seq(jar1, jar2, jar3, jar4).map(j => j.toString).mkString(",")
    val args = Seq(
      "--class", SparkSubmitClassLoaderTest.getClass.getName.stripSuffix("$"),
      "--name", "SparkSubmitClassLoaderTest",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--jars", jarsString,
      unusedJar.toString, "SparkSubmitClassA", "SparkSubmitClassB")
    runSparkSubmit(args)
  }

  test("SPARK-8020: set sql conf in spark conf") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val args = Seq(
      "--class", SparkSQLConfTest.getClass.getName.stripSuffix("$"),
      "--name", "SparkSQLConfTest",
      "--master", "local-cluster[2,1,1024]",
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      unusedJar.toString)
    runSparkSubmit(args)
  }

  test("SPARK-8489: MissingRequirementError during reflection") {
    // This test uses a pre-built jar to test SPARK-8489. In a nutshell, this test creates
    // a HiveContext and uses it to create a data frame from an RDD using reflection.
    // Before the fix in SPARK-8470, this results in a MissingRequirementError because
    // the HiveContext code mistakenly overrides the class loader that contains user classes.
    // For more detail, see sql/hive/src/test/resources/regression-test-SPARK-8489/*scala.
    val testJar = "sql/hive/src/test/resources/regression-test-SPARK-8489/test.jar"
    val args = Seq(
      "--conf", "spark.ui.enabled=false",
      "--conf", "spark.master.rest.enabled=false",
      "--class", "Main",
      testJar)
    runSparkSubmit(args)
  }

  test("SPARK-9757 Persist Parquet relation with decimal column") {
    val unusedJar = TestUtils.createJarWithClasses(Seq.empty)
    val args = Seq(
      "--class", SPARK_9757.getClass.getName.stripSuffix("$"),
      "--name", "SparkSQLConfTest",
      "--master", "local-cluster[2,1,1024]",
      unusedJar.toString)
    runSparkSubmit(args)
  }

  // NOTE: This is an expensive operation in terms of time (10 seconds+). Use sparingly.
  // This is copied from org.apache.spark.deploy.SparkSubmitSuite
  private def runSparkSubmit(args: Seq[String]): Unit = {
    val sparkHome = sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    val history = ArrayBuffer.empty[String]
    val commands = Seq("./bin/spark-submit") ++ args
    val commandLine = commands.mkString("'", "' '", "'")

    val builder = new ProcessBuilder(commands: _*).directory(new File(sparkHome))
    val env = builder.environment()
    env.put("SPARK_TESTING", "1")
    env.put("SPARK_HOME", sparkHome)

    def captureOutput(source: String)(line: String): Unit = {
      // This test suite has some weird behaviors when executed on Jenkins:
      //
      // 1. Sometimes it gets extremely slow out of unknown reason on Jenkins.  Here we add a
      //    timestamp to provide more diagnosis information.
      // 2. Log lines are not correctly redirected to unit-tests.log as expected, so here we print
      //    them out for debugging purposes.
      val logLine = s"${new Timestamp(new Date().getTime)} - $source> $line"
      // scalastyle:off println
      println(logLine)
      // scalastyle:on println
      history += logLine
    }

    val process = builder.start()
    new ProcessOutputCapturer(process.getInputStream, captureOutput("stdout")).start()
    new ProcessOutputCapturer(process.getErrorStream, captureOutput("stderr")).start()

    try {
      val exitCode = failAfter(180.seconds) { process.waitFor() }
      if (exitCode != 0) {
        // include logs in output. Note that logging is async and may not have completed
        // at the time this exception is raised
        Thread.sleep(1000)
        val historyLog = history.mkString("\n")
        fail {
          s"""spark-submit returned with exit code $exitCode.
             |Command line: $commandLine
             |
             |$historyLog
           """.stripMargin
        }
      }
    } catch {
      case to: TestFailedDueToTimeoutException =>
        val historyLog = history.mkString("\n")
        fail(s"Timeout of $commandLine" +
            s" See the log4j logs for more detail." +
            s"\n$historyLog", to)
        case t: Throwable => throw t
    } finally {
      // Ensure we still kill the process in case it timed out
      process.destroy()
    }
  }
}

// This object is used for testing SPARK-8368: https://issues.apache.org/jira/browse/SPARK-8368.
// We test if we can load user jars in both driver and executors when HiveContext is used.
object SparkSubmitClassLoaderTest extends Logging {
  def main(args: Array[String]) {
    Utils.configTestLog4j("INFO")
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val hiveContext = new TestHiveContext(sc)
    val df = hiveContext.createDataFrame((1 to 100).map(i => (i, i))).toDF("i", "j")
    logInfo("Testing load classes at the driver side.")
    // First, we load classes at driver side.
    try {
      Utils.classForName(args(0))
      Utils.classForName(args(1))
    } catch {
      case t: Throwable =>
        throw new Exception("Could not load user class from jar:\n", t)
    }
    // Second, we load classes at the executor side.
    logInfo("Testing load classes at the executor side.")
    val result = df.mapPartitions { x =>
      var exception: String = null
      try {
        Utils.classForName(args(0))
        Utils.classForName(args(1))
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

    // Load a Hive UDF from the jar.
    logInfo("Registering temporary Hive UDF provided in a jar.")
    hiveContext.sql(
      """
        |CREATE TEMPORARY FUNCTION example_max
        |AS 'org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleMax'
      """.stripMargin)
    val source =
      hiveContext.createDataFrame((1 to 10).map(i => (i, s"str$i"))).toDF("key", "val")
    source.registerTempTable("sourceTable")
    // Load a Hive SerDe from the jar.
    logInfo("Creating a Hive table with a SerDe provided in a jar.")
    hiveContext.sql(
      """
        |CREATE TABLE t1(key int, val string)
        |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
      """.stripMargin)
    // Actually use the loaded UDF and SerDe.
    logInfo("Writing data into the table.")
    hiveContext.sql(
      "INSERT INTO TABLE t1 SELECT example_max(key) as key, val FROM sourceTable GROUP BY val")
    logInfo("Running a simple query on the table.")
    val count = hiveContext.table("t1").orderBy("key", "val").count()
    if (count != 10) {
      throw new Exception(s"table t1 should have 10 rows instead of $count rows")
    }
    logInfo("Test finishes.")
    sc.stop()
  }
}

// This object is used for testing SPARK-8020: https://issues.apache.org/jira/browse/SPARK-8020.
// We test if we can correctly set spark sql configurations when HiveContext is used.
object SparkSQLConfTest extends Logging {
  def main(args: Array[String]) {
    Utils.configTestLog4j("INFO")
    // We override the SparkConf to add spark.sql.hive.metastore.version and
    // spark.sql.hive.metastore.jars to the beginning of the conf entry array.
    // So, if metadataHive get initialized after we set spark.sql.hive.metastore.version but
    // before spark.sql.hive.metastore.jars get set, we will see the following exception:
    // Exception in thread "main" java.lang.IllegalArgumentException: Builtin jars can only
    // be used when hive execution version == hive metastore version.
    // Execution: 0.13.1 != Metastore: 0.12. Specify a valid path to the correct hive jars
    // using $HIVE_METASTORE_JARS or change spark.sql.hive.metastore.version to 0.13.1.
    val conf = new SparkConf() {
      override def getAll: Array[(String, String)] = {
        def isMetastoreSetting(conf: String): Boolean = {
          conf == "spark.sql.hive.metastore.version" || conf == "spark.sql.hive.metastore.jars"
        }
        // If there is any metastore settings, remove them.
        val filteredSettings = super.getAll.filterNot(e => isMetastoreSetting(e._1))

        // Always add these two metastore settings at the beginning.
        ("spark.sql.hive.metastore.version" -> "0.12") +:
        ("spark.sql.hive.metastore.jars" -> "maven") +:
        filteredSettings
      }

      // For this simple test, we do not really clone this object.
      override def clone: SparkConf = this
    }
    val sc = new SparkContext(conf)
    val hiveContext = new TestHiveContext(sc)
    // Run a simple command to make sure all lazy vals in hiveContext get instantiated.
    hiveContext.tables().collect()
    sc.stop()
  }
}

object SPARK_9757 extends QueryTest with Logging {
  def main(args: Array[String]): Unit = {
    Utils.configTestLog4j("INFO")

    val sparkContext = new SparkContext(
      new SparkConf()
        .set("spark.sql.hive.metastore.version", "0.13.1")
        .set("spark.sql.hive.metastore.jars", "maven"))

    val hiveContext = new TestHiveContext(sparkContext)
    import hiveContext.implicits._

    import org.apache.spark.sql.functions._

    val dir = Utils.createTempDir()
    dir.delete()

    try {
      {
        val df =
          hiveContext
            .range(10)
            .select(('id + 0.1) cast DecimalType(10, 3) as 'dec)
        df.write.option("path", dir.getCanonicalPath).mode("overwrite").saveAsTable("t")
        checkAnswer(hiveContext.table("t"), df)
      }

      {
        val df =
          hiveContext
            .range(10)
            .select(callUDF("struct", ('id + 0.2) cast DecimalType(10, 3)) as 'dec_struct)
        df.write.option("path", dir.getCanonicalPath).mode("overwrite").saveAsTable("t")
        checkAnswer(hiveContext.table("t"), df)
      }
    } finally {
      dir.delete()
      hiveContext.sql("DROP TABLE t")
      sparkContext.stop()
    }
  }
}
