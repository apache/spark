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
import java.net.URL
import java.sql.{Date, DriverManager, Statement}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.sys.process.{Process, ProcessLogger}
import scala.util.{Random, Try}

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.jdbc.HiveDriver
import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.hive.service.cli.GetInfoType
import org.apache.hive.service.cli.thrift.TCLIService.Client
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveShim
import org.apache.spark.util.Utils

object TestData {
  def getTestDataFilePath(name: String): URL = {
    Thread.currentThread().getContextClassLoader.getResource(s"data/files/$name")
  }

  val smallKv = getTestDataFilePath("small_kv.txt")
  val smallKvWithNull = getTestDataFilePath("small_kv_with_null.txt")
}

class HiveThriftBinaryServerSuite extends HiveThriftJdbcTest {
  override def mode: ServerMode.Value = ServerMode.binary

  private def withCLIServiceClient(f: ThriftCLIServiceClient => Unit): Unit = {
    // Transport creation logics below mimics HiveConnection.createBinaryTransport
    val rawTransport = new TSocket("localhost", serverPort)
    val user = System.getProperty("user.name")
    val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
    val protocol = new TBinaryProtocol(transport)
    val client = new ThriftCLIServiceClient(new Client(protocol))

    transport.open()
    try f(client) finally transport.close()
  }

  test("GetInfo Thrift API") {
    withCLIServiceClient { client =>
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

  test("JDBC query execution") {
    withJdbcStatement { statement =>
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

  test("Checks Hive version") {
    withJdbcStatement { statement =>
      val resultSet = statement.executeQuery("SET spark.sql.hive.version")
      resultSet.next()
      assert(resultSet.getString(1) === s"spark.sql.hive.version=${HiveShim.version}")
    }
  }

  test("SPARK-3004 regression: result set containing NULL") {
    withJdbcStatement { statement =>
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

  test("SPARK-4292 regression: result set iterator issue") {
    withJdbcStatement { statement =>
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
    withJdbcStatement { statement =>
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
    withJdbcStatement { statement =>
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

  test("test multiple session") {
    import org.apache.spark.sql.SQLConf
    var defaultV1: String = null
    var defaultV2: String = null

    withMultipleConnectionJdbcStatement(
      // create table
      { statement =>

        val queries = Seq(
            "DROP TABLE IF EXISTS test_map",
            "CREATE TABLE test_map(key INT, value STRING)",
            s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_map",
            "CACHE TABLE test_table AS SELECT key FROM test_map ORDER BY key DESC")

        queries.foreach(statement.execute)

        val rs1 = statement.executeQuery("SELECT key FROM test_table ORDER BY KEY DESC")
        val buf1 = new collection.mutable.ArrayBuffer[Int]()
        while (rs1.next()) {
          buf1 += rs1.getInt(1)
        }
        rs1.close()

        val rs2 = statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
        val buf2 = new collection.mutable.ArrayBuffer[Int]()
        while (rs2.next()) {
          buf2 += rs2.getInt(1)
        }
        rs2.close()

        assert(buf1 === buf2)
      },

      // first session, we get the default value of the session status
      { statement =>

        val rs1 = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS}")
        rs1.next()
        defaultV1 = rs1.getString(1)
        assert(defaultV1 != "200")
        rs1.close()

        val rs2 = statement.executeQuery("SET hive.cli.print.header")
        rs2.next()

        defaultV2 = rs2.getString(1)
        assert(defaultV1 != "true")
        rs2.close()
      },

      // second session, we update the session status
      { statement =>

        val queries = Seq(
            s"SET ${SQLConf.SHUFFLE_PARTITIONS}=291",
            "SET hive.cli.print.header=true"
            )

        queries.map(statement.execute)
        val rs1 = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS}")
        rs1.next()
        assert("spark.sql.shuffle.partitions=291" === rs1.getString(1))
        rs1.close()

        val rs2 = statement.executeQuery("SET hive.cli.print.header")
        rs2.next()
        assert("hive.cli.print.header=true" === rs2.getString(1))
        rs2.close()
      },

      // third session, we get the latest session status, supposed to be the
      // default value
      { statement =>

        val rs1 = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS}")
        rs1.next()
        assert(defaultV1 === rs1.getString(1))
        rs1.close()

        val rs2 = statement.executeQuery("SET hive.cli.print.header")
        rs2.next()
        assert(defaultV2 === rs2.getString(1))
        rs2.close()
      },

      // accessing the cached data in another session
      { statement =>

        val rs1 = statement.executeQuery("SELECT key FROM test_table ORDER BY KEY DESC")
        val buf1 = new collection.mutable.ArrayBuffer[Int]()
        while (rs1.next()) {
          buf1 += rs1.getInt(1)
        }
        rs1.close()

        val rs2 = statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
        val buf2 = new collection.mutable.ArrayBuffer[Int]()
        while (rs2.next()) {
          buf2 += rs2.getInt(1)
        }
        rs2.close()

        assert(buf1 === buf2)
        statement.executeQuery("UNCACHE TABLE test_table")

        // TODO need to figure out how to determine if the data loaded from cache
        val rs3 = statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
        val buf3 = new collection.mutable.ArrayBuffer[Int]()
        while (rs3.next()) {
          buf3 += rs3.getInt(1)
        }
        rs3.close()

        assert(buf1 === buf3)
      },

      // accessing the uncached table
      { statement =>

        // TODO need to figure out how to determine if the data loaded from cache
        val rs1 = statement.executeQuery("SELECT key FROM test_table ORDER BY KEY DESC")
        val buf1 = new collection.mutable.ArrayBuffer[Int]()
        while (rs1.next()) {
          buf1 += rs1.getInt(1)
        }
        rs1.close()

        val rs2 = statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
        val buf2 = new collection.mutable.ArrayBuffer[Int]()
        while (rs2.next()) {
          buf2 += rs2.getInt(1)
        }
        rs2.close()

        assert(buf1 === buf2)
      }
    )
  }
}

class HiveThriftHttpServerSuite extends HiveThriftJdbcTest {
  override def mode: ServerMode.Value = ServerMode.http

  test("JDBC query execution") {
    withJdbcStatement { statement =>
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

  test("Checks Hive version") {
    withJdbcStatement { statement =>
      val resultSet = statement.executeQuery("SET spark.sql.hive.version")
      resultSet.next()
      assert(resultSet.getString(1) === s"spark.sql.hive.version=${HiveShim.version}")
    }
  }
}

object ServerMode extends Enumeration {
  val binary, http = Value
}

abstract class HiveThriftJdbcTest extends HiveThriftServer2Test {
  Class.forName(classOf[HiveDriver].getCanonicalName)

  private def jdbcUri = if (mode == ServerMode.http) {
    s"""jdbc:hive2://localhost:$serverPort/
       |default?
       |hive.server2.transport.mode=http;
       |hive.server2.thrift.http.path=cliservice
     """.stripMargin.split("\n").mkString.trim
  } else {
    s"jdbc:hive2://localhost:$serverPort/"
  }

  def withMultipleConnectionJdbcStatement(fs: (Statement => Unit)*) {
    val user = System.getProperty("user.name")
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUri, user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).map { case (s, f) => f(s) }
    } finally {
      statements.map(_.close())
      connections.map(_.close())
    }
  }

  def withJdbcStatement(f: Statement => Unit) {
    withMultipleConnectionJdbcStatement(f)
  }
}

abstract class HiveThriftServer2Test extends FunSuite with BeforeAndAfterAll with Logging {
  def mode: ServerMode.Value

  private val CLASS_NAME = HiveThriftServer2.getClass.getCanonicalName.stripSuffix("$")
  private val LOG_FILE_MARK = s"starting $CLASS_NAME, logging to "

  private val startScript = "../../sbin/start-thriftserver.sh".split("/").mkString(File.separator)
  private val stopScript = "../../sbin/stop-thriftserver.sh".split("/").mkString(File.separator)

  private var listeningPort: Int = _
  protected def serverPort: Int = listeningPort

  protected def user = System.getProperty("user.name")

  private var warehousePath: File = _
  private var metastorePath: File = _
  private def metastoreJdbcUri = s"jdbc:derby:;databaseName=$metastorePath;create=true"

  private val pidDir: File = Utils.createTempDir("thriftserver-pid")
  private var logPath: File = _
  private var logTailingProcess: Process = _
  private var diagnosisBuffer: ArrayBuffer[String] = ArrayBuffer.empty[String]

  private def serverStartCommand(port: Int) = {
    val portConf = if (mode == ServerMode.binary) {
      ConfVars.HIVE_SERVER2_THRIFT_PORT
    } else {
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT
    }

    s"""$startScript
       |  --master local
       |  --hiveconf hive.root.logger=INFO,console
       |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$metastoreJdbcUri
       |  --hiveconf ${ConfVars.METASTOREWAREHOUSE}=$warehousePath
       |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST}=localhost
       |  --hiveconf ${ConfVars.HIVE_SERVER2_TRANSPORT_MODE}=$mode
       |  --hiveconf $portConf=$port
       |  --driver-class-path ${sys.props("java.class.path")}
       |  --conf spark.ui.enabled=false
     """.stripMargin.split("\\s+").toSeq
  }

  private def startThriftServer(port: Int, attempt: Int) = {
    warehousePath = Utils.createTempDir()
    warehousePath.delete()
    metastorePath = Utils.createTempDir()
    metastorePath.delete()
    logPath = null
    logTailingProcess = null

    val command = serverStartCommand(port)

    diagnosisBuffer ++=
      s"""
         |### Attempt $attempt ###
         |HiveThriftServer2 command line: $command
         |Listening port: $port
         |System user: $user
       """.stripMargin.split("\n")

    logInfo(s"Trying to start HiveThriftServer2: port=$port, mode=$mode, attempt=$attempt")

    val env = Seq(
      // Disables SPARK_TESTING to exclude log4j.properties in test directories.
      "SPARK_TESTING" -> "0",
      // Points SPARK_PID_DIR to SPARK_HOME, otherwise only 1 Thrift server instance can be started
      // at a time, which is not Jenkins friendly.
      "SPARK_PID_DIR" -> pidDir.getCanonicalPath)

    logPath = Process(command, None, env: _*).lines.collectFirst {
      case line if line.contains(LOG_FILE_MARK) => new File(line.drop(LOG_FILE_MARK.length))
    }.getOrElse {
      throw new RuntimeException("Failed to find HiveThriftServer2 log file.")
    }

    val serverStarted = Promise[Unit]()

    // Ensures that the following "tail" command won't fail.
    logPath.createNewFile()
    logTailingProcess =
      // Using "-n +0" to make sure all lines in the log file are checked.
      Process(s"/usr/bin/env tail -n +0 -f ${logPath.getCanonicalPath}").run(ProcessLogger(
        (line: String) => {
          diagnosisBuffer += line

          if (line.contains("ThriftBinaryCLIService listening on") ||
              line.contains("Started ThriftHttpCLIService in http")) {
            serverStarted.trySuccess(())
          } else if (line.contains("HiveServer2 is stopped")) {
            // This log line appears when the server fails to start and terminates gracefully (e.g.
            // because of port contention).
            serverStarted.tryFailure(new RuntimeException("Failed to start HiveThriftServer2"))
          }
        }))

    Await.result(serverStarted.future, 2.minute)
  }

  private def stopThriftServer(): Unit = {
    // The `spark-daemon.sh' script uses kill, which is not synchronous, have to wait for a while.
    Process(stopScript, None, "SPARK_PID_DIR" -> pidDir.getCanonicalPath).run().exitValue()
    Thread.sleep(3.seconds.toMillis)

    warehousePath.delete()
    warehousePath = null

    metastorePath.delete()
    metastorePath = null

    Option(logPath).foreach(_.delete())
    logPath = null

    Option(logTailingProcess).foreach(_.destroy())
    logTailingProcess = null
  }

  private def dumpLogs(): Unit = {
    logError(
      s"""
         |=====================================
         |HiveThriftServer2Suite failure output
         |=====================================
         |${diagnosisBuffer.mkString("\n")}
         |=========================================
         |End HiveThriftServer2Suite failure output
         |=========================================
       """.stripMargin)
  }

  override protected def beforeAll(): Unit = {
    // Chooses a random port between 10000 and 19999
    listeningPort = 10000 + Random.nextInt(10000)
    diagnosisBuffer.clear()

    // Retries up to 3 times with different port numbers if the server fails to start
    (1 to 3).foldLeft(Try(startThriftServer(listeningPort, 0))) { case (started, attempt) =>
      started.orElse {
        listeningPort += 1
        stopThriftServer()
        Try(startThriftServer(listeningPort, attempt))
      }
    }.recover {
      case cause: Throwable =>
        dumpLogs()
        throw cause
    }.get

    logInfo(s"HiveThriftServer2 started successfully")
  }

  override protected def afterAll(): Unit = {
    stopThriftServer()
    logInfo("HiveThriftServer2 stopped")
  }
}
