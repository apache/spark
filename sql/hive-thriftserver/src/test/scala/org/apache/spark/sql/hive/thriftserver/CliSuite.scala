/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.Logging
import org.apache.spark.util.Utils

class CliSuite extends FunSuite with BeforeAndAfterAll with Logging {
  def runCliWithin(
      timeout: FiniteDuration,
      extraArgs: Seq[String] = Seq.empty)(
      queriesAndExpectedAnswers: (String, String)*) {

    val (queries, expectedAnswers) = queriesAndExpectedAnswers.unzip
    val warehousePath = Utils.createTempDir()
    warehousePath.delete()
    val metastorePath = Utils.createTempDir()
    metastorePath.delete()
    val cliScript = "../../bin/spark-sql".split("/").mkString(File.separator)

    val command = {
      val jdbcUrl = s"jdbc:derby:;databaseName=$metastorePath;create=true"
      s"""$cliScript
         |  --master local
         |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$jdbcUrl
         |  --hiveconf ${ConfVars.METASTOREWAREHOUSE}=$warehousePath
         |  --driver-class-path ${sys.props("java.class.path")}
       """.stripMargin.split("\\s+").toSeq ++ extraArgs
    }

    var next = 0
    val foundAllExpectedAnswers = Promise.apply[Unit]()
    val queryStream = new ByteArrayInputStream(queries.mkString("\n").getBytes)
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
      }
    }

    // Searching expected output line from both stdout and stderr of the CLI process
    val process = (Process(command, None) #< queryStream).run(
      ProcessLogger(captureOutput("stdout"), captureOutput("stderr")))

    try {
      Await.result(foundAllExpectedAnswers.future, timeout)
    } catch { case cause: Throwable =>
      logError(
        s"""
           |=======================
           |CliSuite failure output
           |=======================
           |Spark SQL CLI command line: ${command.mkString(" ")}
           |
           |Executed query $next "${queries(next)}",
           |But failed to capture expected output "${expectedAnswers(next)}" within $timeout.
           |
           |${buffer.mkString("\n")}
           |===========================
           |End CliSuite failure output
           |===========================
         """.stripMargin, cause)
      throw cause
    } finally {
      warehousePath.delete()
      metastorePath.delete()
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
        -> "Time taken: "
    )
  }

  test("Single command with -e") {
    runCliWithin(1.minute, Seq("-e", "SHOW DATABASES;"))("" -> "OK")
  }
}
