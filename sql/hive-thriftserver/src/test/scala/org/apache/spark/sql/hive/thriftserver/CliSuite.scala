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

package org.apache.spark.sql.hive.thriftserver

import java.io._
import java.sql.Timestamp
import java.util.Date

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import org.apache.spark.sql.test.ProcessTestUtils.ProcessOutputCapturer

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkFunSuite}

/**
 * A test suite for the `spark-sql` CLI tool.  Note that all test cases share the same temporary
 * Hive metastore and warehouse.
 */
class CliSuite extends SparkFunSuite with BeforeAndAfterAll with Logging {
  val warehousePath = Utils.createTempDir()
  val metastorePath = Utils.createTempDir()
  val scratchDirPath = Utils.createTempDir()

  override def beforeAll(): Unit = {
    super.beforeAll()
    warehousePath.delete()
    metastorePath.delete()
    scratchDirPath.delete()
  }

  override def afterAll(): Unit = {
    try {
      warehousePath.delete()
      metastorePath.delete()
      scratchDirPath.delete()
    } finally {
      super.afterAll()
    }
  }

  /**
   * Run a CLI operation and expect all the queries and expected answers to be returned.
   * @param timeout maximum time for the commands to complete
   * @param extraArgs any extra arguments
   * @param errorResponses a sequence of strings whose presence in the stdout of the forked process
   *                       is taken as an immediate error condition. That is: if a line containing
   *                       with one of these strings is found, fail the test immediately.
   *                       The default value is `Seq("Error:")`
   *
   * @param queriesAndExpectedAnswers one or more tupes of query + answer
   */
  def runCliWithin(
      timeout: FiniteDuration,
      extraArgs: Seq[String] = Seq.empty,
      errorResponses: Seq[String] = Seq("Error:"))(
      queriesAndExpectedAnswers: (String, String)*): Unit = {

    val (queries, expectedAnswers) = queriesAndExpectedAnswers.unzip
    // Explicitly adds ENTER for each statement to make sure they are actually entered into the CLI.
    val queriesString = queries.map(_ + "\n").mkString

    val command = {
      val cliScript = "../../bin/spark-sql".split("/").mkString(File.separator)
      val jdbcUrl = s"jdbc:derby:;databaseName=$metastorePath;create=true"
      s"""$cliScript
         |  --master local
         |  --driver-java-options -Dderby.system.durability=test
         |  --conf spark.ui.enabled=false
         |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$jdbcUrl
         |  --hiveconf ${ConfVars.METASTOREWAREHOUSE}=$warehousePath
         |  --hiveconf ${ConfVars.SCRATCHDIR}=$scratchDirPath
       """.stripMargin.split("\\s+").toSeq ++ extraArgs
    }

    var next = 0
    val foundAllExpectedAnswers = Promise.apply[Unit]()
    val buffer = new ArrayBuffer[String]()
    val lock = new Object

    def captureOutput(source: String)(line: String): Unit = lock.synchronized {
      // This test suite sometimes gets extremely slow out of unknown reason on Jenkins.  Here we
      // add a timestamp to provide more diagnosis information.
      buffer += s"${new Timestamp(new Date().getTime)} - $source> $line"

      // If we haven't found all expected answers and another expected answer comes up...
      if (next < expectedAnswers.size && line.contains(expectedAnswers(next))) {
        next += 1
        // If all expected answers have been found...
        if (next == expectedAnswers.size) {
          foundAllExpectedAnswers.trySuccess(())
        }
      } else {
        errorResponses.foreach { r =>
          if (line.contains(r)) {
            foundAllExpectedAnswers.tryFailure(
              new RuntimeException(s"Failed with error line '$line'"))
          }
        }
      }
    }

    val process = new ProcessBuilder(command: _*).start()

    val stdinWriter = new OutputStreamWriter(process.getOutputStream)
    stdinWriter.write(queriesString)
    stdinWriter.flush()
    stdinWriter.close()

    new ProcessOutputCapturer(process.getInputStream, captureOutput("stdout")).start()
    new ProcessOutputCapturer(process.getErrorStream, captureOutput("stderr")).start()

    try {
      Await.result(foundAllExpectedAnswers.future, timeout)
    } catch { case cause: Throwable =>
      val message =
        s"""
           |=======================
           |CliSuite failure output
           |=======================
           |Spark SQL CLI command line: ${command.mkString(" ")}
           |Exception: $cause
           |Executed query $next "${queries(next)}",
           |But failed to capture expected output "${expectedAnswers(next)}" within $timeout.
           |
           |${buffer.mkString("\n")}
           |===========================
           |End CliSuite failure output
           |===========================
         """.stripMargin
      logError(message, cause)
      fail(message, cause)
    } finally {
      process.destroy()
    }
  }

  test("Simple commands") {
    val dataFilePath =
      Thread.currentThread().getContextClassLoader.getResource("data/files/small_kv.txt")

    runCliWithin(3.minute)(
      "CREATE TABLE hive_test(key INT, val STRING);"
        -> "OK",
      "SHOW TABLES;"
        -> "hive_test",
      s"LOAD DATA LOCAL INPATH '$dataFilePath' OVERWRITE INTO TABLE hive_test;"
        -> "OK",
      "CACHE TABLE hive_test;"
        -> "",
      "SELECT COUNT(*) FROM hive_test;"
        -> "5",
      "DROP TABLE hive_test;"
        -> "OK"
    )
  }

  test("Single command with -e") {
    runCliWithin(2.minute, Seq("-e", "SHOW DATABASES;"))("" -> "OK")
  }

  test("Single command with --database") {
    runCliWithin(2.minute)(
      "CREATE DATABASE hive_test_db;"
        -> "OK",
      "USE hive_test_db;"
        -> "OK",
      "CREATE TABLE hive_test(key INT, val STRING);"
        -> "OK",
      "SHOW TABLES;"
        -> "hive_test"
    )

    runCliWithin(2.minute, Seq("--database", "hive_test_db", "-e", "SHOW TABLES;"))(
      ""
        -> "OK",
      ""
        -> "hive_test"
    )
  }

  test("Commands using SerDe provided in --jars") {
    val jarFile =
      "../hive/src/test/resources/hive-hcatalog-core-0.13.1.jar"
        .split("/")
        .mkString(File.separator)

    val dataFilePath =
      Thread.currentThread().getContextClassLoader.getResource("data/files/small_kv.txt")

    runCliWithin(3.minute, Seq("--jars", s"$jarFile"))(
      """CREATE TABLE t1(key string, val string)
        |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe';
      """.stripMargin
        -> "OK",
      "CREATE TABLE sourceTable (key INT, val STRING);"
        -> "OK",
      s"LOAD DATA LOCAL INPATH '$dataFilePath' OVERWRITE INTO TABLE sourceTable;"
        -> "OK",
      "INSERT INTO TABLE t1 SELECT key, val FROM sourceTable;"
        -> "",
      "SELECT count(key) FROM t1;"
        -> "5",
      "DROP TABLE t1;"
        -> "OK",
      "DROP TABLE sourceTable;"
        -> "OK"
    )
  }

  test("SPARK-11188 Analysis error reporting") {
    runCliWithin(timeout = 2.minute,
      errorResponses = Seq("AnalysisException"))(
      "select * from nonexistent_table;"
        -> "Error in query: Table not found: nonexistent_table;"
    )
  }

  test("SPARK-11624 Spark SQL CLI should set sessionState only once") {
    runCliWithin(2.minute, Seq("-e", "!echo \"This is a test for Spark-11624\";"))(
      "" -> "This is a test for Spark-11624")
  }
}
