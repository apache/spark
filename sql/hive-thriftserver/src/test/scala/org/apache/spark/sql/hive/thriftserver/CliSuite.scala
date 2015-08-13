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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Failure

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.scalatest.BeforeAndAfter

import org.apache.spark.{Logging, SparkFunSuite}
import org.apache.spark.util.Utils

/**
 * A test suite for the `spark-sql` CLI tool.  Note that all test cases share the same temporary
 * Hive metastore and warehouse.
 */
class CliSuite extends SparkFunSuite with BeforeAndAfter with Logging {
  val warehousePath = Utils.createTempDir()
  val metastorePath = Utils.createTempDir()
  val scratchDirPath = Utils.createTempDir()

  before {
    warehousePath.delete()
    metastorePath.delete()
    scratchDirPath.delete()
  }

  after {
    warehousePath.delete()
    metastorePath.delete()
    scratchDirPath.delete()
  }

  /**
   * Run a CLI operation and expect all the queries and expected answers to be returned.
   * @param timeout maximum time for the commands to complete
   * @param extraArgs any extra arguments
   * @param errorResponses a sequence of strings whose presence in the stdout of the forked process
   *                       is taken as an immediate error condition. That is: if a line beginning
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
    val command = {
      val cliScript = "../../bin/spark-sql".split("/").mkString(File.separator)
      val jdbcUrl = s"jdbc:derby:;databaseName=$metastorePath;create=true"
      s"""$cliScript
         |  --master local
         |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$jdbcUrl
         |  --hiveconf ${ConfVars.METASTOREWAREHOUSE}=$warehousePath
         |  --hiveconf ${ConfVars.SCRATCHDIR}=$scratchDirPath
       """.stripMargin.split("\\s+").toSeq ++ extraArgs
    }

    var next = 0
    val foundAllExpectedAnswers = Promise.apply[Unit]()
    // Explicitly adds ENTER for each statement to make sure they are actually entered into the CLI.
    val queryStream = new ByteArrayInputStream(queries.map(_ + "\n").mkString.getBytes)
    val buffer = new ArrayBuffer[String]()
    val lock = new Object

    def captureOutput(source: String)(line: String): Unit = lock.synchronized {
      buffer += s"$source> $line"
      // If we haven't found all expected answers and another expected answer comes up...
      if (next < expectedAnswers.size && line.startsWith(expectedAnswers(next))) {
        next += 1
        // If all expected answers have been found...
        if (next == expectedAnswers.size) {
          foundAllExpectedAnswers.trySuccess(())
        }
      } else {
        errorResponses.foreach( r => {
          if (line.startsWith(r)) {
            foundAllExpectedAnswers.tryFailure(
              new RuntimeException(s"Failed with error line '$line'"))
          }})
      }
    }

    // Searching expected output line from both stdout and stderr of the CLI process
    val process = (Process(command, None) #< queryStream).run(
      ProcessLogger(captureOutput("stdout"), captureOutput("stderr")))

    // catch the output value
    class exitCodeCatcher extends Runnable {
      var exitValue = 0

      override def run(): Unit = {
        try {
          exitValue = process.exitValue()
        } catch {
          case rte: RuntimeException =>
            // ignored as it will get triggered when the process gets destroyed
            logDebug("Ignoring exception while waiting for exit code", rte)
        }
        if (exitValue != 0) {
          // process exited: fail fast
          foundAllExpectedAnswers.tryFailure(
            new RuntimeException(s"Failed with exit code $exitValue"))
        }
      }
    }
    // spin off the code catche thread. No attempt is made to kill this
    // as it will exit once the launched process terminates.
    val codeCatcherThread = new Thread(new exitCodeCatcher())
    codeCatcherThread.start()

    try {
      Await.ready(foundAllExpectedAnswers.future, timeout)
      foundAllExpectedAnswers.future.value match {
        case Some(Failure(t)) => throw t
        case _ =>
      }
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
        -> "Time taken: ",
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
        -> "Time taken: "
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
        -> "Time taken:",
      "SELECT count(key) FROM t1;"
        -> "5",
      "DROP TABLE t1;"
        -> "OK",
      "DROP TABLE sourceTable;"
        -> "OK"
    )
  }
}
