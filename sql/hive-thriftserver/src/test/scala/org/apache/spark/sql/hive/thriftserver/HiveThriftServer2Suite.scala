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
import java.sql.{Date, DriverManager, Statement}
import java.util.concurrent.TimeoutException

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.jdbc.HiveDriver
import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.hive.service.cli.GetInfoType
import org.apache.hive.service.cli.thrift.TCLIService.Client
import org.apache.hive.service.cli.thrift._
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import org.scalatest.FunSuite

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.util.getTempFilePath
import org.apache.spark.sql.hive.HiveShim

/**
 * Tests for the HiveThriftServer2 using JDBC.
 *
 * NOTE: SPARK_PREPEND_CLASSES is explicitly disabled in this test suite. Assembly jar must be
 * rebuilt after changing HiveThriftServer2 related code.
 */
class HiveThriftServer2Suite extends FunSuite with Logging {
  Class.forName(classOf[HiveDriver].getCanonicalName)

  object TestData {
    def getTestDataFilePath(name: String) = {
      Thread.currentThread().getContextClassLoader.getResource(s"data/files/$name")
    }

    val smallKv = getTestDataFilePath("small_kv.txt")
    val smallKvWithNull = getTestDataFilePath("small_kv_with_null.txt")
  }

  def randomListeningPort =  {
    // Let the system to choose a random available port to avoid collision with other parallel
    // builds.
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  def withJdbcStatement(
      serverStartTimeout: FiniteDuration = 1.minute,
      httpMode: Boolean = false)(
      f: Statement => Unit) {
    val port = randomListeningPort

    startThriftServer(port, serverStartTimeout, httpMode) {
      val jdbcUri = if (httpMode) {
        s"jdbc:hive2://${"localhost"}:$port/" +
          "default?hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice"
      } else {
        s"jdbc:hive2://${"localhost"}:$port/"
      }

      val user = System.getProperty("user.name")
      val connection = DriverManager.getConnection(jdbcUri, user, "")
      val statement = connection.createStatement()

      try {
        f(statement)
      } finally {
        statement.close()
        connection.close()
      }
    }
  }

  def withCLIServiceClient(
      serverStartTimeout: FiniteDuration = 1.minute)(
      f: ThriftCLIServiceClient => Unit) {
    val port = randomListeningPort

    startThriftServer(port) {
      // Transport creation logics below mimics HiveConnection.createBinaryTransport
      val rawTransport = new TSocket("localhost", port)
      val user = System.getProperty("user.name")
      val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
      val protocol = new TBinaryProtocol(transport)
      val client = new ThriftCLIServiceClient(new Client(protocol))

      transport.open()

      try {
        f(client)
      } finally {
        transport.close()
      }
    }
  }

  def startThriftServer(
      port: Int,
      serverStartTimeout: FiniteDuration = 1.minute,
      httpMode: Boolean = false)(
      f: => Unit) {
    val startScript = "../../sbin/start-thriftserver.sh".split("/").mkString(File.separator)
    val stopScript = "../../sbin/stop-thriftserver.sh".split("/").mkString(File.separator)

    val warehousePath = getTempFilePath("warehouse")
    val metastorePath = getTempFilePath("metastore")
    val metastoreJdbcUri = s"jdbc:derby:;databaseName=$metastorePath;create=true"

    val command =
      if (httpMode) {
          s"""$startScript
             |  --master local
             |  --hiveconf hive.root.logger=INFO,console
             |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$metastoreJdbcUri
             |  --hiveconf ${ConfVars.METASTOREWAREHOUSE}=$warehousePath
             |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST}=localhost
             |  --hiveconf ${ConfVars.HIVE_SERVER2_TRANSPORT_MODE}=http
             |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT}=$port
           """.stripMargin.split("\\s+").toSeq
      } else {
          s"""$startScript
             |  --master local
             |  --hiveconf hive.root.logger=INFO,console
             |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$metastoreJdbcUri
             |  --hiveconf ${ConfVars.METASTOREWAREHOUSE}=$warehousePath
             |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST}=localhost
             |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_PORT}=$port
           """.stripMargin.split("\\s+").toSeq
      }

    val serverRunning = Promise[Unit]()
    val buffer = new ArrayBuffer[String]()
    val LOGGING_MARK =
      s"starting ${HiveThriftServer2.getClass.getCanonicalName.stripSuffix("$")}, logging to "
    var logTailingProcess: Process = null
    var logFilePath: String = null

    def captureLogOutput(line: String): Unit = {
      buffer += line
      if (line.contains("ThriftBinaryCLIService listening on") ||
          line.contains("Started ThriftHttpCLIService in http")) {
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
    val env = Seq("SPARK_TESTING" -> "0")

    Process(command, None, env: _*).run(ProcessLogger(
      captureThriftServerOutput("stdout"),
      captureThriftServerOutput("stderr")))

    try {
      Await.result(serverRunning.future, serverStartTimeout)
      f
    } catch {
      case cause: Exception =>
        cause match {
          case _: TimeoutException =>
            logError(s"Failed to start Hive Thrift server within $serverStartTimeout", cause)
          case _ =>
        }
        logError(
          s"""
             |=====================================
             |HiveThriftServer2Suite failure output
             |=====================================
             |HiveThriftServer2 command line: ${command.mkString(" ")}
             |Binding port: $port
             |System user: ${System.getProperty("user.name")}
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
    withJdbcStatement() { statement =>
      val queries = Seq(
        "SET spark.sql.shuffle.partitions=3",
        "DROP TABLE IF EXISTS test",
        "CREATE TABLE test(key INT, val STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test",
        "CACHE TABLE test")

      queries.foreach(statement.execute)

      assertResult(5, "Row count mismatch") {
        val resultSet = statement.executeQuery("SELECT COUNT(*) FROM test")
        resultSet.next()
        resultSet.getInt(1)
      }
    }
  }

  test("Test JDBC query execution in Http Mode") {
    withJdbcStatement(httpMode = true) { statement =>
      val queries = Seq(
        "SET spark.sql.shuffle.partitions=3",
        "DROP TABLE IF EXISTS test",
        "CREATE TABLE test(key INT, val STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test",
        "CACHE TABLE test")

      queries.foreach(statement.execute)

      assertResult(5, "Row count mismatch") {
        val resultSet = statement.executeQuery("SELECT COUNT(*) FROM test")
        resultSet.next()
        resultSet.getInt(1)
      }
    }
  }

  test("SPARK-3004 regression: result set containing NULL") {
    withJdbcStatement() { statement =>
      val queries = Seq(
        "DROP TABLE IF EXISTS test_null",
        "CREATE TABLE test_null(key INT, val STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKvWithNull}' OVERWRITE INTO TABLE test_null")

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

  test("GetInfo Thrift API") {
    withCLIServiceClient() { client =>
      val user = System.getProperty("user.name")
      val sessionHandle = client.openSession(user, "")

      assertResult("Spark SQL", "Wrong GetInfo(CLI_DBMS_NAME) result") {
        client.getInfo(sessionHandle, GetInfoType.CLI_DBMS_NAME).getStringValue
      }

      assertResult("Spark SQL", "Wrong GetInfo(CLI_SERVER_NAME) result") {
        client.getInfo(sessionHandle, GetInfoType.CLI_SERVER_NAME).getStringValue
      }

      assertResult(true, "Spark version shouldn't be \"Unknown\"") {
        val version = client.getInfo(sessionHandle, GetInfoType.CLI_DBMS_VER).getStringValue
        logInfo(s"Spark version: $version")
        version != "Unknown"
      }
    }
  }

  test("Checks Hive version") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SET spark.sql.hive.version")
      resultSet.next()
      assert(resultSet.getString(1) === s"spark.sql.hive.version=${HiveShim.version}")
    }
  }

  test("Checks Hive version in Http Mode") {
    withJdbcStatement(httpMode = true) { statement =>
      val resultSet = statement.executeQuery("SET spark.sql.hive.version")
      resultSet.next()
      assert(resultSet.getString(1) === s"spark.sql.hive.version=${HiveShim.version}")
    }
  }

  test("SPARK-4292 regression: result set iterator issue") {
    withJdbcStatement() { statement =>
      val queries = Seq(
        "DROP TABLE IF EXISTS test_4292",
        "CREATE TABLE test_4292(key INT, val STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_4292")

      queries.foreach(statement.execute)

      val resultSet = statement.executeQuery("SELECT key FROM test_4292")

      Seq(238, 86, 311, 27, 165).foreach { key =>
        resultSet.next()
        assert(resultSet.getInt(1) === key)
      }

      statement.executeQuery("DROP TABLE IF EXISTS test_4292")
    }
  }

  test("SPARK-4309 regression: Date type support") {
    withJdbcStatement() { statement =>
      val queries = Seq(
        "DROP TABLE IF EXISTS test_date",
        "CREATE TABLE test_date(key INT, value STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_date")

      queries.foreach(statement.execute)

      assertResult(Date.valueOf("2011-01-01")) {
        val resultSet = statement.executeQuery(
          "SELECT CAST('2011-01-01' as date) FROM test_date LIMIT 1")
        resultSet.next()
        resultSet.getDate(1)
      }
    }
  }

  test("SPARK-4407 regression: Complex type support") {
    withJdbcStatement() { statement =>
      val queries = Seq(
        "DROP TABLE IF EXISTS test_map",
        "CREATE TABLE test_map(key INT, value STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_map")

      queries.foreach(statement.execute)

      assertResult("""{238:"val_238"}""") {
        val resultSet = statement.executeQuery("SELECT MAP(key, value) FROM test_map LIMIT 1")
        resultSet.next()
        resultSet.getString(1)
      }

      assertResult("""["238","val_238"]""") {
        val resultSet = statement.executeQuery(
          "SELECT ARRAY(CAST(key AS STRING), value) FROM test_map LIMIT 1")
        resultSet.next()
        resultSet.getString(1)
      }
    }
  }
}
