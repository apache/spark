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

import java.io.File
import java.net.ServerSocket
import java.sql.{DriverManager, Statement}
import java.util.concurrent.TimeoutException

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.jdbc.HiveDriver
import org.scalatest.FunSuite

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.util.getTempFilePath

/**
 * Tests for the HiveThriftServer2 using JDBC.
 */
class HiveThriftServer2Suite extends FunSuite with Logging {
  Class.forName(classOf[HiveDriver].getCanonicalName)

  def startThriftServerWithin(timeout: FiniteDuration = 1.minute)(f: Statement => Unit) {
    val startScript = "../../sbin/start-thriftserver.sh".split("/").mkString(File.separator)
    val stopScript = "../../sbin/stop-thriftserver.sh".split("/").mkString(File.separator)

    val warehousePath = getTempFilePath("warehouse")
    val metastorePath = getTempFilePath("metastore")
    val metastoreJdbcUri = s"jdbc:derby:;databaseName=$metastorePath;create=true"
    val listeningHost = "localhost"
    val listeningPort =  {
      // Let the system to choose a random available port to avoid collision with other parallel
      // builds.
      val socket = new ServerSocket(0)
      val port = socket.getLocalPort
      socket.close()
      port
    }

    val command =
      s"""$startScript
         |  --master local
         |  --hiveconf hive.root.logger=INFO,console
         |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$metastoreJdbcUri
         |  --hiveconf ${ConfVars.METASTOREWAREHOUSE}=$warehousePath
         |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST}=$listeningHost
         |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_PORT}=$listeningPort
       """.stripMargin.split("\\s+").toSeq

    val serverRunning = Promise[Unit]()
    val buffer = new ArrayBuffer[String]()
    val LOGGING_MARK =
      s"starting ${HiveThriftServer2.getClass.getCanonicalName.stripSuffix("$")}, logging to "
    var logTailingProcess: Process = null
    var logFilePath: String = null

    def captureLogOutput(line: String): Unit = {
      buffer += line
      if (line.contains("ThriftBinaryCLIService listening on")) {
        serverRunning.success(())
      }
    }

    def captureThriftServerOutput(source: String)(line: String): Unit = {
      if (line.startsWith(LOGGING_MARK)) {
        logFilePath = line.drop(LOGGING_MARK.length).trim
        // Ensure that the log file is created so that the `tail' command won't fail
        Try(new File(logFilePath).createNewFile())
        logTailingProcess = Process(s"/usr/bin/env tail -f $logFilePath")
          .run(ProcessLogger(captureLogOutput, _ => ()))
      }
    }

    // Resets SPARK_TESTING to avoid loading Log4J configurations in testing class paths
    Process(command, None, "SPARK_TESTING" -> "0").run(ProcessLogger(
      captureThriftServerOutput("stdout"),
      captureThriftServerOutput("stderr")))

    val jdbcUri = s"jdbc:hive2://$listeningHost:$listeningPort/"
    val user = System.getProperty("user.name")

    try {
      Await.result(serverRunning.future, timeout)

      val connection = DriverManager.getConnection(jdbcUri, user, "")
      val statement = connection.createStatement()

      try {
        f(statement)
      } finally {
        statement.close()
        connection.close()
      }
    } catch {
      case cause: Exception =>
        cause match {
          case _: TimeoutException =>
            logError(s"Failed to start Hive Thrift server within $timeout", cause)
          case _ =>
        }
        logError(
          s"""
             |=====================================
             |HiveThriftServer2Suite failure output
             |=====================================
             |HiveThriftServer2 command line: ${command.mkString(" ")}
             |JDBC URI: $jdbcUri
             |User: $user
             |
             |${buffer.mkString("\n")}
             |=========================================
             |End HiveThriftServer2Suite failure output
             |=========================================
           """.stripMargin, cause)
        throw cause
    } finally {
      warehousePath.delete()
      metastorePath.delete()
      Process(stopScript).run().exitValue()
      // The `spark-daemon.sh' script uses kill, which is not synchronous, have to wait for a while.
      Thread.sleep(3.seconds.toMillis)
      Option(logTailingProcess).map(_.destroy())
      Option(logFilePath).map(new File(_).delete())
    }
  }

  test("Test JDBC query execution") {
    startThriftServerWithin() { statement =>
      val dataFilePath =
        Thread.currentThread().getContextClassLoader.getResource("data/files/small_kv.txt")

      val queries =
        s"""SET spark.sql.shuffle.partitions=3;
           |CREATE TABLE test(key INT, val STRING);
           |LOAD DATA LOCAL INPATH '$dataFilePath' OVERWRITE INTO TABLE test;
           |CACHE TABLE test;
         """.stripMargin.split(";").map(_.trim).filter(_.nonEmpty)

      queries.foreach(statement.execute)

      assertResult(5, "Row count mismatch") {
        val resultSet = statement.executeQuery("SELECT COUNT(*) FROM test")
        resultSet.next()
        resultSet.getInt(1)
      }
    }
  }

  test("SPARK-3004 regression: result set containing NULL") {
    startThriftServerWithin() { statement =>
      val dataFilePath =
        Thread.currentThread().getContextClassLoader.getResource(
          "data/files/small_kv_with_null.txt")

      val queries = Seq(
        "DROP TABLE IF EXISTS test_null",
        "CREATE TABLE test_null(key INT, val STRING)",
        s"LOAD DATA LOCAL INPATH '$dataFilePath' OVERWRITE INTO TABLE test_null")

      queries.foreach(statement.execute)

      val resultSet = statement.executeQuery("SELECT * FROM test_null WHERE key IS NULL")

      (0 until 5).foreach { _ =>
        resultSet.next()
        assert(resultSet.getInt(1) === 0)
        assert(resultSet.wasNull())
      }

      assert(!resultSet.next())
    }
  }
}
