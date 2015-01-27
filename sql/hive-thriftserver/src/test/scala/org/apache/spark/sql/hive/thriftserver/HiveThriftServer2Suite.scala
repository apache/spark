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

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.jdbc.HiveDriver
import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.hive.service.cli.GetInfoType
import org.apache.hive.service.cli.thrift.TCLIService.Client
import org.apache.hive.service.cli.thrift._
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import org.scalatest.FunSuite

import org.apache.spark.{SparkContext, Logging, SPARK_VERSION}
import org.apache.spark.sql.catalyst.util.getTempFilePath

/**
 * Tests for the HiveThriftServer2 using JDBC.
 *
 * NOTE: SPARK_PREPEND_CLASSES is explicitly disabled in this test suite. Assembly jar must be
 * rebuilt after changing HiveThriftServer2 related code.
 */
class HiveThriftServer2Suite extends FunSuite with Logging {
  Class.forName(classOf[HiveDriver].getCanonicalName)

  def randomListeningPort =  {
    // Let the system to choose a random available port to avoid collision with other parallel
    // builds.
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  def withJdbcStatement(serverStartTimeout: FiniteDuration = 1.minute)(f: Statement => Unit) {
    val port = randomListeningPort

    startThriftServer(port, serverStartTimeout) {
      val jdbcUri = s"jdbc:hive2://${"localhost"}:$port/"
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
      serverStartTimeout: FiniteDuration = 1.minute)(
      f: => Unit) {
    val startScript = "../../sbin/start-thriftserver.sh".split("/").mkString(File.separator)

    val warehousePath = getTempFilePath("warehouse")
    val metastorePath = getTempFilePath("metastore")
    val metastoreJdbcUri = s"jdbc:derby:;databaseName=$metastorePath;create=true"
    val command =
      s"""$startScript
         |  --master local
         |  --hiveconf hive.root.logger=INFO,console
         |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$metastoreJdbcUri
         |  --hiveconf ${ConfVars.METASTOREWAREHOUSE}=$warehousePath
         |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST}=${"localhost"}
         |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_PORT}=$port
         |  --conf spark.ui.enabled=false
       """.stripMargin.split("\\s+").toSeq

    val serverRunning = Promise[Unit]()
    val buffer = new ArrayBuffer[String]()
    val lock = new Object

    def captureOutput(source: String)(line: String): Unit = lock.synchronized {
      buffer += s"$source> $line"
      if (line.contains("ThriftBinaryCLIService listening on")) {
        serverRunning.success(())
      }
    }

    // Resets SPARK_TESTING to avoid loading Log4J configurations in testing class paths
    val env = Seq("SPARK_TESTING" -> "0")

    val process = Process(command, None, env: _*).run(
      ProcessLogger(captureOutput("stdout"), captureOutput("stderr")))

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
      process.destroy()
    }
  }

  test("Test JDBC query execution") {
    withJdbcStatement() { statement =>
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
    withJdbcStatement() { statement =>
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

      assertResult(SPARK_VERSION, "Spark version shouldn't be \"Unknown\"") {
        client.getInfo(sessionHandle, GetInfoType.CLI_DBMS_VER).getStringValue
      }
    }
  }

  test("Checks Hive version") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SET spark.sql.hive.version")
      resultSet.next()
      assert(resultSet.getString(1) === s"spark.sql.hive.version=0.12.0")
    }
  }
}
