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

import java.io.{File, FilenameFilter}
import java.net.URL
import java.nio.charset.StandardCharsets
import java.sql.{Date, DriverManager, SQLException, Statement}
import java.util.{Locale, UUID}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future, Promise}
import scala.concurrent.duration._
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Try

import com.google.common.io.Files
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.jdbc.HiveDriver
import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.hive.service.cli.{CLIService, FetchOrientation, FetchType, GetInfoType, RowSetFactory}
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient
import org.apache.hive.service.rpc.thrift.TCLIService.Client
import org.apache.hive.service.rpc.thrift.TRowSet
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ProcessTestUtils.ProcessOutputCapturer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.HiveTestJars
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.HIVE_THRIFT_SERVER_SINGLESESSION
import org.apache.spark.util.{ShutdownHookManager, ThreadUtils, Utils}

object TestData {
  def getTestDataFilePath(name: String): URL = {
    Thread.currentThread().getContextClassLoader.getResource(s"data/files/$name")
  }

  val smallKv = getTestDataFilePath("small_kv.txt")
  val smallKvWithNull = getTestDataFilePath("small_kv_with_null.txt")
}

class HiveThriftBinaryServerSuite extends HiveThriftServer2Test {
  override def mode: ServerMode.Value = ServerMode.binary

  private def withCLIServiceClient(f: ThriftCLIServiceClient => Unit): Unit = {
    // Transport creation logic below mimics HiveConnection.createBinaryTransport
    val rawTransport = new TSocket(localhost, serverPort)
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

  test("SPARK-16563 ThriftCLIService FetchResults repeat fetching result") {
    withCLIServiceClient { client =>
      val user = System.getProperty("user.name")
      val sessionHandle = client.openSession(user, "")

      withJdbcStatement("test_16563") { statement =>
        val queries = Seq(
          "CREATE TABLE test_16563(key INT, val STRING) USING hive",
          s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_16563")

        queries.foreach(statement.execute)
        val confOverlay = new java.util.HashMap[java.lang.String, java.lang.String]
        val operationHandle = client.executeStatement(
          sessionHandle,
          "SELECT * FROM test_16563",
          confOverlay)

        // Fetch result first time
        assertResult(5, "Fetching result first time from next row") {

          val rows_next = client.fetchResults(
            operationHandle,
            FetchOrientation.FETCH_NEXT,
            1000,
            FetchType.QUERY_OUTPUT)

          RowSetFactory.create(rows_next, sessionHandle.getProtocolVersion).numRows()
        }

        // Fetch result second time from first row
        assertResult(5, "Repeat fetching result from first row") {

          val rows_first = client.fetchResults(
            operationHandle,
            FetchOrientation.FETCH_FIRST,
            1000,
            FetchType.QUERY_OUTPUT)

          RowSetFactory.create(rows_first, sessionHandle.getProtocolVersion).numRows()
        }
      }
    }
  }

  test("Support beeline --hiveconf and --hivevar") {
    withJdbcStatement() { statement =>
      executeTest(hiveConfList)
      executeTest(hiveVarList)
      def executeTest(hiveList: String): Unit = {
        hiveList.split(";").foreach{ m =>
          val kv = m.split("=")
          val k = kv(0)
          val v = kv(1)
          val modValue = s"${v}_MOD_VALUE"
          // select '${a}'; ---> avalue
          val resultSet = statement.executeQuery(s"select '$${$k}'")
          resultSet.next()
          assert(resultSet.getString(1) === v)
          statement.executeQuery(s"set $k=$modValue")
          val modResultSet = statement.executeQuery(s"select '$${$k}'")
          modResultSet.next()
          assert(modResultSet.getString(1) === s"$modValue")
        }
      }
    }
  }

  test("JDBC query execution") {
    withJdbcStatement("test") { statement =>
      val queries = Seq(
        "SET spark.sql.shuffle.partitions=3",
        "CREATE TABLE test(key INT, val STRING) USING hive",
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
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SET spark.sql.hive.version")
      resultSet.next()
      assert(resultSet.getString(1) === "spark.sql.hive.version")
      assert(resultSet.getString(2) === HiveUtils.builtinHiveVersion)
    }
  }

  test("SPARK-3004 regression: result set containing NULL") {
    withJdbcStatement("test_null") { statement =>
      val queries = Seq(
        "CREATE TABLE test_null(key INT, val STRING) USING hive",
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
    withJdbcStatement("test_4292") { statement =>
      val queries = Seq(
        "CREATE TABLE test_4292(key INT, val STRING) USING hive",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_4292")

      queries.foreach(statement.execute)

      val resultSet = statement.executeQuery("SELECT key FROM test_4292")

      Seq(238, 86, 311, 27, 165).foreach { key =>
        resultSet.next()
        assert(resultSet.getInt(1) === key)
      }
    }
  }

  test("SPARK-4309 regression: Date type support") {
    withJdbcStatement("test_date") { statement =>
      val queries = Seq(
        "CREATE TABLE test_date(key INT, value STRING) USING hive",
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
    withJdbcStatement("test_map") { statement =>
      val queries = Seq(
        "CREATE TABLE test_map(key INT, value STRING) USING hive",
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

  test("SPARK-12143 regression: Binary type support") {
    withJdbcStatement("test_binary") { statement =>
      val queries = Seq(
        "CREATE TABLE test_binary(key INT, value STRING) USING hive",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_binary")

      queries.foreach(statement.execute)

      val expected: Array[Byte] = "val_238".getBytes
      assertResult(expected) {
        val resultSet = statement.executeQuery(
          "SELECT CAST(value as BINARY) FROM test_binary LIMIT 1")
        resultSet.next()
        resultSet.getObject(1)
      }
    }
  }

  test("test multiple session") {
    var defaultV1: String = null
    var defaultV2: String = null
    var data: ArrayBuffer[Int] = null

    withMultipleConnectionJdbcStatement("test_map", "db1.test_map2")(
      // create table
      { statement =>

        val queries = Seq(
            "CREATE TABLE test_map(key INT, value STRING) USING hive",
            s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_map",
            "CACHE TABLE test_table AS SELECT key FROM test_map ORDER BY key DESC",
            "CREATE DATABASE db1")

        queries.foreach(statement.execute)

        val plan = statement.executeQuery("explain select * from test_table")
        plan.next()
        plan.next()
        assert(plan.getString(1).contains("Scan In-memory table test_table"))

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

        data = buf1
      },

      // first session, we get the default value of the session status
      { statement =>

        val rs1 = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}")
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
            s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}=291",
            "SET hive.cli.print.header=true"
            )

        queries.map(statement.execute)
        val rs1 = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}")
        rs1.next()
        assert("spark.sql.shuffle.partitions" === rs1.getString(1))
        assert("291" === rs1.getString(2))
        rs1.close()

        val rs2 = statement.executeQuery("SET hive.cli.print.header")
        rs2.next()
        assert("hive.cli.print.header" === rs2.getString(1))
        assert("true" === rs2.getString(2))
        rs2.close()
      },

      // third session, we get the latest session status, supposed to be the
      // default value
      { statement =>

        val rs1 = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}")
        rs1.next()
        assert(defaultV1 === rs1.getString(1))
        rs1.close()

        val rs2 = statement.executeQuery("SET hive.cli.print.header")
        rs2.next()
        assert(defaultV2 === rs2.getString(1))
        rs2.close()
      },

      // try to access the cached data in another session
      { statement =>

        // Cached temporary table can't be accessed by other sessions
        intercept[SQLException] {
          statement.executeQuery("SELECT key FROM test_table ORDER BY KEY DESC")
        }

        // The cached temporary table can be used indirectly if the query matches.
        val plan = statement.executeQuery("explain select key from test_map ORDER BY key DESC")
        plan.next()
        plan.next()
        assert(plan.getString(1).contains("Scan In-memory table test_table"))

        val rs = statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
        val buf = new collection.mutable.ArrayBuffer[Int]()
        while (rs.next()) {
          buf += rs.getInt(1)
        }
        rs.close()
        assert(buf === data)
      },

      // switch another database
      { statement =>
        statement.execute("USE db1")

        // there is no test_map table in db1
        intercept[SQLException] {
          statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
        }

        statement.execute("CREATE TABLE test_map2(key INT, value STRING)")
      },

      // access default database
      { statement =>

        // current database should still be `default`
        intercept[SQLException] {
          statement.executeQuery("SELECT key FROM test_map2")
        }

        statement.execute("USE db1")
        // access test_map2
        statement.executeQuery("SELECT key from test_map2")
      }
    )
  }

  // This test often hangs and then times out, leaving the hanging processes.
  // Let's ignore it and improve the test.
  ignore("test jdbc cancel") {
    withJdbcStatement("test_map") { statement =>
      val queries = Seq(
        "CREATE TABLE test_map(key INT, value STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_map")

      queries.foreach(statement.execute)
      implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(
        ThreadUtils.newDaemonSingleThreadExecutor("test-jdbc-cancel"))
      try {
        // Start a very-long-running query that will take hours to finish, then cancel it in order
        // to demonstrate that cancellation works.
        val f = Future {
          statement.executeQuery(
            "SELECT COUNT(*) FROM test_map " +
            List.fill(10)("join test_map").mkString(" "))
        }
        // Note that this is slightly race-prone: if the cancel is issued before the statement
        // begins executing then we'll fail with a timeout. As a result, this fixed delay is set
        // slightly more conservatively than may be strictly necessary.
        Thread.sleep(1000)
        statement.cancel()
        val e = intercept[SparkException] {
          ThreadUtils.awaitResult(f, 3.minute)
        }.getCause
        assert(e.isInstanceOf[SQLException])
        assert(e.getMessage.contains("cancelled"))

        // Cancellation is a no-op if spark.sql.hive.thriftServer.async=false
        statement.executeQuery("SET spark.sql.hive.thriftServer.async=false")
        try {
          val sf = Future {
            statement.executeQuery(
              "SELECT COUNT(*) FROM test_map " +
                List.fill(4)("join test_map").mkString(" ")
            )
          }
          // Similarly, this is also slightly race-prone on fast machines where the query above
          // might race and complete before we issue the cancel.
          Thread.sleep(1000)
          statement.cancel()
          val rs1 = ThreadUtils.awaitResult(sf, 3.minute)
          rs1.next()
          assert(rs1.getInt(1) === math.pow(5, 5))
          rs1.close()

          val rs2 = statement.executeQuery("SELECT COUNT(*) FROM test_map")
          rs2.next()
          assert(rs2.getInt(1) === 5)
          rs2.close()
        } finally {
          statement.executeQuery("SET spark.sql.hive.thriftServer.async=true")
        }
      } finally {
        ec.shutdownNow()
      }
    }
  }

  test("test add jar") {
    withMultipleConnectionJdbcStatement("smallKV", "addJar")(
      {
        statement =>
          val jarFile = HiveTestJars.getHiveHcatalogCoreJar().getCanonicalPath

          statement.executeQuery(s"ADD JAR $jarFile")
      },

      {
        statement =>
          val queries = Seq(
            "CREATE TABLE smallKV(key INT, val STRING) USING hive",
            s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE smallKV",
            """CREATE TABLE addJar(key string)
              |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
            """.stripMargin)

          queries.foreach(statement.execute)

          statement.executeQuery(
            """
              |INSERT INTO TABLE addJar SELECT 'k1' as key FROM smallKV limit 1
            """.stripMargin)

          val actualResult =
            statement.executeQuery("SELECT key FROM addJar")
          val actualResultBuffer = new collection.mutable.ArrayBuffer[String]()
          while (actualResult.next()) {
            actualResultBuffer += actualResult.getString(1)
          }
          actualResult.close()

          val expectedResult =
            statement.executeQuery("SELECT 'k1'")
          val expectedResultBuffer = new collection.mutable.ArrayBuffer[String]()
          while (expectedResult.next()) {
            expectedResultBuffer += expectedResult.getString(1)
          }
          expectedResult.close()

          assert(expectedResultBuffer === actualResultBuffer)
      }
    )
  }

  test("Checks Hive version via SET -v") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SET -v")

      val conf = mutable.Map.empty[String, String]
      while (resultSet.next()) {
        conf += resultSet.getString(1) -> resultSet.getString(2)
      }

      assert(conf.get(HiveUtils.BUILTIN_HIVE_VERSION.key) === Some(HiveUtils.builtinHiveVersion))
    }
  }

  test("Checks Hive version via SET") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SET")

      val conf = mutable.Map.empty[String, String]
      while (resultSet.next()) {
        conf += resultSet.getString(1) -> resultSet.getString(2)
      }

      assert(conf.get(HiveUtils.BUILTIN_HIVE_VERSION.key) === Some(HiveUtils.builtinHiveVersion))
    }
  }

  test("SPARK-11595 ADD JAR with input path having URL scheme") {
    withJdbcStatement("test_udtf") { statement =>
      try {
        val jarPath = "../hive/src/test/resources/TestUDTF.jar"
        val jarURL = s"file://${System.getProperty("user.dir")}/$jarPath"

        Seq(
          s"ADD JAR $jarURL",
          s"""CREATE TEMPORARY FUNCTION udtf_count2
             |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
           """.stripMargin
        ).foreach(statement.execute)

        val rs1 = statement.executeQuery("DESCRIBE FUNCTION udtf_count2")

        assert(rs1.next())
        assert(rs1.getString(1) === "Function: udtf_count2")

        assert(rs1.next())
        assertResult("Class: org.apache.spark.sql.hive.execution.GenericUDTFCount2") {
          rs1.getString(1)
        }

        assert(rs1.next())
        assert(rs1.getString(1) === "Usage: N/A.")

        val dataPath = "../hive/src/test/resources/data/files/kv1.txt"

        Seq(
          "CREATE TABLE test_udtf(key INT, value STRING) USING hive",
          s"LOAD DATA LOCAL INPATH '$dataPath' OVERWRITE INTO TABLE test_udtf"
        ).foreach(statement.execute)

        val rs2 = statement.executeQuery(
          "SELECT key, cc FROM test_udtf LATERAL VIEW udtf_count2(value) dd AS cc")

        assert(rs2.next())
        assert(rs2.getInt(1) === 97)
        assert(rs2.getInt(2) === 500)

        assert(rs2.next())
        assert(rs2.getInt(1) === 97)
        assert(rs2.getInt(2) === 500)
      } finally {
        statement.executeQuery("DROP TEMPORARY FUNCTION udtf_count2")
      }
    }
  }

  test("SPARK-11043 check operation log root directory") {
    val expectedLine =
      "Operation log root directory is created: " + operationLogPath.getAbsoluteFile
    val bufferSrc = Source.fromFile(logPath)
    Utils.tryWithSafeFinally {
      assert(bufferSrc.getLines().exists(_.contains(expectedLine)))
    } {
      bufferSrc.close()
    }
  }

  test("SPARK-23547 Cleanup the .pipeout file when the Hive Session closed") {
    def pipeoutFileList(sessionID: UUID): Array[File] = {
      lScratchDir.listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          name.startsWith(sessionID.toString) && name.endsWith(".pipeout")
        }
      })
    }

    withCLIServiceClient { client =>
      val user = System.getProperty("user.name")
      val sessionHandle = client.openSession(user, "")
      val sessionID = sessionHandle.getSessionId
      assert(pipeoutFileList(sessionID) === null)

      client.closeSession(sessionHandle)

      assert(pipeoutFileList(sessionID) === null)
    }
  }

  test("SPARK-24829 Checks cast as float") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT CAST('4.56' AS FLOAT)")
      resultSet.next()
      assert(resultSet.getString(1) === "4.56")
    }
  }

  test("SPARK-28463: Thriftserver throws BigDecimal incompatible with HiveDecimal") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT CAST(1 AS decimal(38, 18))")
      assert(rs.next())
      assert(rs.getBigDecimal(1) === new java.math.BigDecimal("1.000000000000000000"))
    }
  }

  test("Support interval type") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT interval 5 years 7 months")
      assert(rs.next())
      assert(rs.getString(1) === "5-7")
    }
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT interval 8 days 10 hours 5 minutes 10 seconds")
      assert(rs.next())
      assert(rs.getString(1) === "8 10:05:10.000000000")
    }
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT interval 3 days 1 hours")
      assert(rs.next())
      assert(rs.getString(1) === "3 01:00:00.000000000")
    }

    // Invalid interval value
    withJdbcStatement() { statement =>
      val e = intercept[SQLException] {
        statement.executeQuery("SELECT interval 5 yea 7 months")
      }
      assert(e.getMessage.contains("org.apache.spark.sql.catalyst.parser.ParseException"))
    }
    withJdbcStatement() { statement =>
      val e = intercept[SQLException] {
        statement.executeQuery("SELECT interval 8 days 10 hours 5 minutes 10 secon")
      }
      assert(e.getMessage.contains("org.apache.spark.sql.catalyst.parser.ParseException"))
    }
    withJdbcStatement() { statement =>
      val e = intercept[SQLException] {
        statement.executeQuery("SELECT interval 3 months 1 hou")
      }
      assert(e.getMessage.contains("org.apache.spark.sql.catalyst.parser.ParseException"))
    }

    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT interval '3-1' year to month;")
      assert(rs.next())
      assert(rs.getString(1) === "3-1")
    }

    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT interval '3 1:1:1' day to second;")
      assert(rs.next())
      assert(rs.getString(1) === "3 01:01:01.000000000")
    }
  }

  test("Query Intervals in VIEWs through thrift server") {
    val viewName1 = "view_interval_1"
    val viewName2 = "view_interval_2"
    val ddl1 =
      s"""
         |CREATE GLOBAL TEMP VIEW $viewName1
         |AS SELECT
         | INTERVAL 1 DAY AS a,
         | INTERVAL '2-1' YEAR TO MONTH AS b,
         | INTERVAL '3 1:1:1' DAY TO SECOND AS c
       """.stripMargin
    val ddl2 = s"CREATE TEMP VIEW $viewName2 as select * from global_temp.$viewName1"
    withJdbcStatement(viewName1, viewName2) { statement =>
      statement.executeQuery(ddl1)
      statement.executeQuery(ddl2)
      val rs = statement.executeQuery(
        s"""
           |SELECT v1.a AS a1, v2.a AS a2,
           | v1.b AS b1, v2.b AS b2,
           | v1.c AS c1, v2.c AS c2
           |FROM global_temp.$viewName1 v1
           |JOIN $viewName2 v2
           |ON date_part('DAY', v1.a) = date_part('DAY', v2.a)
           |  AND v1.b = v2.b
           |  AND v1.c = v2.c
           |""".stripMargin)
      while (rs.next()) {
        assert(rs.getString("a1") === "1 00:00:00.000000000")
        assert(rs.getString("a2") === "1 00:00:00.000000000")
        assert(rs.getString("b1") === "2-1")
        assert(rs.getString("b2") === "2-1")
        assert(rs.getString("c1") === "3 01:01:01.000000000")
        assert(rs.getString("c2") === "3 01:01:01.000000000")
      }
    }
  }

  test("ThriftCLIService FetchResults FETCH_FIRST, FETCH_NEXT, FETCH_PRIOR") {
    def checkResult(rows: TRowSet, start: Long, end: Long): Unit = {
      val rowSet = RowSetFactory.create(rows, CLIService.SERVER_VERSION)
      assert(rowSet.getStartOffset == start)
      assert(rowSet.numRows() == end - start)
      rowSet.iterator.asScala.zip((start until end).iterator).foreach { case (row, v) =>
        assert(row(0).asInstanceOf[Long] === v)
      }
    }

    withCLIServiceClient { client =>
      val user = System.getProperty("user.name")
      val sessionHandle = client.openSession(user, "")

      val confOverlay = new java.util.HashMap[java.lang.String, java.lang.String]
      val operationHandle = client.executeStatement(
        sessionHandle,
        "SELECT * FROM range(10)",
        confOverlay) // 10 rows result with sequence 0, 1, 2, ..., 9
      var rows: TRowSet = null

      // Fetch 5 rows with FETCH_NEXT
      rows = client.fetchResults(
        operationHandle, FetchOrientation.FETCH_NEXT, 5, FetchType.QUERY_OUTPUT)
      checkResult(rows, 0, 5) // fetched [0, 5)

      // Fetch another 2 rows with FETCH_NEXT
      rows = client.fetchResults(
        operationHandle, FetchOrientation.FETCH_NEXT, 2, FetchType.QUERY_OUTPUT)
      checkResult(rows, 5, 7) // fetched [5, 7)

      // FETCH_PRIOR 3 rows
      rows = client.fetchResults(
        operationHandle, FetchOrientation.FETCH_PRIOR, 3, FetchType.QUERY_OUTPUT)
      checkResult(rows, 2, 5) // fetched [2, 5)

      // FETCH_PRIOR again will scroll back to 0, and then the returned result
      // may overlap the results of previous FETCH_PRIOR
      rows = client.fetchResults(
        operationHandle, FetchOrientation.FETCH_PRIOR, 3, FetchType.QUERY_OUTPUT)
      checkResult(rows, 0, 3) // fetched [0, 3)

      // FETCH_PRIOR again will stay at 0
      rows = client.fetchResults(
        operationHandle, FetchOrientation.FETCH_PRIOR, 4, FetchType.QUERY_OUTPUT)
      checkResult(rows, 0, 4) // fetched [0, 4)

      // FETCH_NEXT will continue moving forward from offset 4
      rows = client.fetchResults(
        operationHandle, FetchOrientation.FETCH_NEXT, 10, FetchType.QUERY_OUTPUT)
      checkResult(rows, 4, 10) // fetched [4, 10) until the end of results

      // FETCH_NEXT is at end of results
      rows = client.fetchResults(
        operationHandle, FetchOrientation.FETCH_NEXT, 5, FetchType.QUERY_OUTPUT)
      checkResult(rows, 10, 10) // fetched empty [10, 10) (at end of results)

      // FETCH_NEXT is at end of results again
      rows = client.fetchResults(
        operationHandle, FetchOrientation.FETCH_NEXT, 2, FetchType.QUERY_OUTPUT)
      checkResult(rows, 10, 10) // fetched empty [10, 10) (at end of results)

      // FETCH_PRIOR 1 rows yet again
      rows = client.fetchResults(
        operationHandle, FetchOrientation.FETCH_PRIOR, 1, FetchType.QUERY_OUTPUT)
      checkResult(rows, 9, 10) // fetched [9, 10)

      // FETCH_NEXT will return 0 yet again
      rows = client.fetchResults(
        operationHandle, FetchOrientation.FETCH_NEXT, 5, FetchType.QUERY_OUTPUT)
      checkResult(rows, 10, 10) // fetched empty [10, 10) (at end of results)

      // FETCH_FIRST results from first row
      rows = client.fetchResults(
        operationHandle, FetchOrientation.FETCH_FIRST, 3, FetchType.QUERY_OUTPUT)
      checkResult(rows, 0, 3) // fetch [0, 3)

      // Fetch till the end rows with FETCH_NEXT"
      rows = client.fetchResults(
        operationHandle, FetchOrientation.FETCH_NEXT, 1000, FetchType.QUERY_OUTPUT)
      checkResult(rows, 3, 10) // fetched [3, 10)

      client.closeOperation(operationHandle)
      client.closeSession(sessionHandle)
    }
  }

  test("SPARK-29492: use add jar in sync mode") {
    withCLIServiceClient { client =>
      val user = System.getProperty("user.name")
      val sessionHandle = client.openSession(user, "")
      withJdbcStatement("smallKV", "addJar") { statement =>
        val confOverlay = new java.util.HashMap[java.lang.String, java.lang.String]
        val jarFile = HiveTestJars.getHiveHcatalogCoreJar().getCanonicalPath

        Seq(s"ADD JAR $jarFile",
          "CREATE TABLE smallKV(key INT, val STRING) USING hive",
          s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE smallKV")
          .foreach(query => client.executeStatement(sessionHandle, query, confOverlay))

        client.executeStatement(sessionHandle,
          """CREATE TABLE addJar(key string)
            |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
          """.stripMargin, confOverlay)

        client.executeStatement(sessionHandle,
          "INSERT INTO TABLE addJar SELECT 'k1' as key FROM smallKV limit 1", confOverlay)

        val operationHandle = client.executeStatement(
          sessionHandle,
          "SELECT key FROM addJar",
          confOverlay)

        // Fetch result first time
        assertResult(1, "Fetching result first time from next row") {

          val rows_next = client.fetchResults(
            operationHandle,
            FetchOrientation.FETCH_NEXT,
            1000,
            FetchType.QUERY_OUTPUT)
          RowSetFactory.create(rows_next, sessionHandle.getProtocolVersion).numRows()
        }
      }
    }
  }

  test("SPARK-31859 Thriftserver works with spark.sql.datetime.java8API.enabled=true") {
    withJdbcStatement() { st =>
      st.execute("set spark.sql.datetime.java8API.enabled=true")
      val rs = st.executeQuery("select date '2020-05-28', timestamp '2020-05-28 00:00:00'")
      rs.next()
      assert(rs.getDate(1).toString() == "2020-05-28")
      assert(rs.getTimestamp(2).toString() == "2020-05-28 00:00:00.0")
    }
  }

  test("SPARK-31861 Thriftserver respects spark.sql.session.timeZone") {
    withJdbcStatement() { st =>
      st.execute("set spark.sql.session.timeZone=+03:15") // different than Thriftserver's JVM tz
      val rs = st.executeQuery("select timestamp '2020-05-28 10:00:00'")
      rs.next()
      // The timestamp as string is the same as the literal
      assert(rs.getString(1) == "2020-05-28 10:00:00.0")
      // Parsing it to java.sql.Timestamp in the client will always result in a timestamp
      // in client default JVM timezone. The string value of the Timestamp will match the literal,
      // but if the JDBC application cares about the internal timezone and UTC offset of the
      // Timestamp object, it should set spark.sql.session.timeZone to match its client JVM tz.
      assert(rs.getTimestamp(1).toString() == "2020-05-28 10:00:00.0")
    }
  }

  test("SPARK-31863 Session conf should persist between Thriftserver worker threads") {
    val iter = 20
    withJdbcStatement() { statement =>
      // date 'now' is resolved during parsing, and relies on SQLConf.get to
      // obtain the current set timezone. We exploit this to run this test.
      // If the timezones are set correctly to 25 hours apart across threads,
      // the dates should reflect this.

      // iterate a few times for the odd chance the same thread is selected
      for (_ <- 0 until iter) {
        statement.execute("SET spark.sql.session.timeZone=GMT-12")
        val firstResult = statement.executeQuery("SELECT date 'now'")
        firstResult.next()
        val beyondDateLineWest = firstResult.getDate(1)

        statement.execute("SET spark.sql.session.timeZone=GMT+13")
        val secondResult = statement.executeQuery("SELECT date 'now'")
        secondResult.next()
        val dateLineEast = secondResult.getDate(1)
        assert(
          dateLineEast after beyondDateLineWest,
          "SQLConf changes should persist across execution threads")
      }
    }
  }

  test("SPARK-30808: use Java 8 time API and Proleptic Gregorian calendar by default") {
    withJdbcStatement() { st =>
      // Proleptic Gregorian calendar has no gap in the range 1582-10-04..1582-10-15
      val date = "1582-10-10"
      val rs = st.executeQuery(s"select date '$date'")
      rs.next()
      val expected = java.sql.Date.valueOf(date)
      assert(rs.getDate(1) === expected)
      assert(rs.getString(1) === expected.toString)
    }
  }

  test("SPARK-26533: Support query auto timeout cancel on thriftserver - setQueryTimeout") {
    withJdbcStatement() { statement =>
      statement.setQueryTimeout(1)
      val e = intercept[SQLException] {
        statement.execute("select java_method('java.lang.Thread', 'sleep', 10000L)")
      }.getMessage
      assert(e.contains("Query timed out after"))

      statement.setQueryTimeout(0)
      val rs1 = statement.executeQuery(
        "select 'test', java_method('java.lang.Thread', 'sleep', 3000L)")
      rs1.next()
      assert(rs1.getString(1) == "test")

      statement.setQueryTimeout(-1)
      val rs2 = statement.executeQuery(
        "select 'test', java_method('java.lang.Thread', 'sleep', 3000L)")
      rs2.next()
      assert(rs2.getString(1) == "test")
    }
  }

  test("SPARK-26533: Support query auto timeout cancel on thriftserver - SQLConf") {
    withJdbcStatement() { statement =>
      statement.execute(s"SET ${SQLConf.THRIFTSERVER_QUERY_TIMEOUT.key}=1")
      val e1 = intercept[SQLException] {
        statement.execute("select java_method('java.lang.Thread', 'sleep', 10000L)")
      }.getMessage
      assert(e1.contains("Query timed out after"))

      statement.execute(s"SET ${SQLConf.THRIFTSERVER_QUERY_TIMEOUT.key}=0")
      val rs = statement.executeQuery(
        "select 'test', java_method('java.lang.Thread', 'sleep', 3000L)")
      rs.next()
      assert(rs.getString(1) == "test")

      // Uses a smaller timeout value of a config value and an a user-specified one
      statement.execute(s"SET ${SQLConf.THRIFTSERVER_QUERY_TIMEOUT.key}=1")
      statement.setQueryTimeout(30)
      val e2 = intercept[SQLException] {
        statement.execute("select java_method('java.lang.Thread', 'sleep', 10000L)")
      }.getMessage
      assert(e2.contains("Query timed out after"))

      statement.execute(s"SET ${SQLConf.THRIFTSERVER_QUERY_TIMEOUT.key}=30")
      statement.setQueryTimeout(1)
      val e3 = intercept[SQLException] {
        statement.execute("select java_method('java.lang.Thread', 'sleep', 10000L)")
      }.getMessage
      assert(e3.contains("Query timed out after"))
    }
  }
}

class SingleSessionSuite extends HiveThriftServer2TestBase {
  override def mode: ServerMode.Value = ServerMode.binary

  override protected def extraConf: Seq[String] =
    s"--conf ${HIVE_THRIFT_SERVER_SINGLESESSION.key}=true" :: Nil

  test("share the temporary functions across JDBC connections") {
    withMultipleConnectionJdbcStatement("test_udtf")(
      { statement =>
        val jarPath = "../hive/src/test/resources/TestUDTF.jar"
        val jarURL = s"file://${System.getProperty("user.dir")}/$jarPath"

        // Configurations and temporary functions added in this session should be visible to all
        // the other sessions.
        Seq(
          "SET foo=bar",
          s"ADD JAR $jarURL",
          "CREATE TABLE test_udtf(key INT, value STRING) USING hive",
          s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_udtf",
          s"""CREATE TEMPORARY FUNCTION udtf_count2
              |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
           """.stripMargin
        ).foreach(statement.execute)
      },

      { statement =>
        try {
          val rs1 = statement.executeQuery("SET foo")

          assert(rs1.next())
          assert(rs1.getString(1) === "foo")
          assert(rs1.getString(2) === "bar")

          val rs2 = statement.executeQuery("DESCRIBE FUNCTION udtf_count2")

          assert(rs2.next())
          assert(rs2.getString(1) === "Function: udtf_count2")

          assert(rs2.next())
          assertResult("Class: org.apache.spark.sql.hive.execution.GenericUDTFCount2") {
            rs2.getString(1)
          }

          assert(rs2.next())
          assert(rs2.getString(1) === "Usage: N/A.")

          val rs3 = statement.executeQuery(
            "SELECT key, cc FROM test_udtf LATERAL VIEW udtf_count2(value) dd AS cc")
          assert(rs3.next())
          assert(rs3.getInt(1) === 165)
          assert(rs3.getInt(2) === 5)

          assert(rs3.next())
          assert(rs3.getInt(1) === 165)
          assert(rs3.getInt(2) === 5)
        } finally {
          statement.executeQuery("DROP TEMPORARY FUNCTION udtf_count2")
        }
      }
    )
  }

  test("unable to changing spark.sql.hive.thriftServer.singleSession using JDBC connections") {
    withJdbcStatement() { statement =>
      // JDBC connections are not able to set the conf spark.sql.hive.thriftServer.singleSession
      val e = intercept[SQLException] {
        statement.executeQuery("SET spark.sql.hive.thriftServer.singleSession=false")
      }.getMessage
      assert(e.contains(
        "Cannot modify the value of a static config: spark.sql.hive.thriftServer.singleSession"))
    }
  }

  test("share the current database and temporary tables across JDBC connections") {
    withMultipleConnectionJdbcStatement()(
      { statement =>
        statement.execute("CREATE DATABASE IF NOT EXISTS db1")
      },

      { statement =>
        val rs1 = statement.executeQuery("SELECT current_database()")
        assert(rs1.next())
        assert(rs1.getString(1) === "default")

        statement.execute("USE db1")

        val rs2 = statement.executeQuery("SELECT current_database()")
        assert(rs2.next())
        assert(rs2.getString(1) === "db1")

        statement.execute("CREATE TEMP VIEW tempView AS SELECT 123")
      },

      { statement =>
        // the current database is set to db1 by another JDBC connection.
        val rs1 = statement.executeQuery("SELECT current_database()")
        assert(rs1.next())
        assert(rs1.getString(1) === "db1")

        val rs2 = statement.executeQuery("SELECT * from tempView")
        assert(rs2.next())
        assert(rs2.getString(1) === "123")

        statement.execute("USE default")
        statement.execute("DROP VIEW tempView")
        statement.execute("DROP DATABASE db1 CASCADE")
      }
    )
  }
}

class HiveThriftCleanUpScratchDirSuite extends HiveThriftServer2TestBase {
  var tempScratchDir: File = _

  override protected def beforeAll(): Unit = {
    tempScratchDir = Utils.createTempDir()
    tempScratchDir.setWritable(true, false)
    assert(tempScratchDir.list().isEmpty)
    new File(tempScratchDir.getAbsolutePath + File.separator + "SPARK-31626").createNewFile()
    assert(tempScratchDir.list().nonEmpty)
    super.beforeAll()
  }

  override def mode: ServerMode.Value = ServerMode.binary

  override protected def extraConf: Seq[String] =
    s" --hiveconf ${ConfVars.HIVE_START_CLEANUP_SCRATCHDIR}=true " ::
       s"--hiveconf hive.exec.scratchdir=${tempScratchDir.getAbsolutePath}" :: Nil

  test("Cleanup the Hive scratchdir when starting the Hive Server") {
    assert(!tempScratchDir.exists())
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT id FROM range(1)")
      assert(rs.next())
      assert(rs.getLong(1) === 0L)
    }
  }

  override protected def afterAll(): Unit = {
    Utils.deleteRecursively(tempScratchDir)
    super.afterAll()
  }
}

class HiveThriftHttpServerSuite extends HiveThriftServer2Test {
  override def mode: ServerMode.Value = ServerMode.http

  test("JDBC query execution") {
    withJdbcStatement("test") { statement =>
      val queries = Seq(
        "SET spark.sql.shuffle.partitions=3",
        "CREATE TABLE test(key INT, val STRING) USING hive",
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
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SET spark.sql.hive.version")
      resultSet.next()
      assert(resultSet.getString(1) === "spark.sql.hive.version")
      assert(resultSet.getString(2) === HiveUtils.builtinHiveVersion)
    }
  }

  test("SPARK-24829 Checks cast as float") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT CAST('4.56' AS FLOAT)")
      resultSet.next()
      assert(resultSet.getString(1) === "4.56")
    }
  }
}

object ServerMode extends Enumeration {
  val binary, http = Value
}

abstract class HiveThriftServer2TestBase extends SparkFunSuite with BeforeAndAfterAll with Logging {
  def mode: ServerMode.Value

  private val CLASS_NAME = HiveThriftServer2.getClass.getCanonicalName.stripSuffix("$")
  private val LOG_FILE_MARK = s"starting $CLASS_NAME, logging to "

  protected val startScript = "../../sbin/start-thriftserver.sh".split("/").mkString(File.separator)
  protected val stopScript = "../../sbin/stop-thriftserver.sh".split("/").mkString(File.separator)

  val localhost = Utils.localCanonicalHostName()
  private var listeningPort: Int = _
  protected def serverPort: Int = listeningPort

  protected val hiveConfList = "a=avalue;b=bvalue"
  protected val hiveVarList = "c=cvalue;d=dvalue"
  protected def user = System.getProperty("user.name")

  protected var warehousePath: File = _
  protected var metastorePath: File = _
  protected def metastoreJdbcUri = s"jdbc:derby:;databaseName=$metastorePath;create=true"

  private val pidDir: File = Utils.createTempDir(namePrefix = "thriftserver-pid")
  protected var logPath: File = _
  protected var operationLogPath: File = _
  protected var lScratchDir: File = _
  private var logTailingProcess: Process = _
  private val diagnosisBuffer: ArrayBuffer[String] = ArrayBuffer.empty[String]

  protected def extraConf: Seq[String] = Nil

  protected def serverStartCommand(): Seq[String] = {
    val portConf = if (mode == ServerMode.binary) {
      ConfVars.HIVE_SERVER2_THRIFT_PORT
    } else {
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT
    }

    val driverClassPath = {
      // Writes a temporary log4j2.properties and prepend it to driver classpath, so that it
      // overrides all other potential log4j configurations contained in other dependency jar files.
      val tempLog4jConf = Utils.createTempDir().getCanonicalPath

      Files.asCharSink(new File(s"$tempLog4jConf/log4j2.properties"), StandardCharsets.UTF_8).write(
        """rootLogger.level = info
          |rootLogger.appenderRef.stdout.ref = console
          |appender.console.type = Console
          |appender.console.name = console
          |appender.console.target = SYSTEM_ERR
          |appender.console.layout.type = PatternLayout
          |appender.console.layout.pattern = %d{HH:mm:ss.SSS} %p %c: %maxLen{%m}{512}%n%ex{8}%n
        """.stripMargin)

      tempLog4jConf
    }

    s"""$startScript
       |  --master local
       |  --hiveconf javax.jdo.option.ConnectionURL=$metastoreJdbcUri
       |  --hiveconf hive.metastore.warehouse.dir=$warehousePath
       |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST}=$localhost
       |  --hiveconf ${ConfVars.HIVE_SERVER2_TRANSPORT_MODE}=$mode
       |  --hiveconf ${ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION}=$operationLogPath
       |  --hiveconf hive.exec.local.scratchdir=$lScratchDir
       |  --hiveconf $portConf=0
       |  --driver-class-path $driverClassPath
       |  --driver-java-options -Dlog4j2.debug
       |  --conf spark.ui.enabled=false
       |  ${extraConf.mkString("\n")}
     """.stripMargin.split("\\s+").toSeq
  }

  /**
   * String to scan for when looking for the thrift binary endpoint running.
   * This can change across Hive versions.
   */
  val THRIFT_BINARY_SERVICE_LIVE = "Starting ThriftBinaryCLIService on port"

  /**
   * String to scan for when looking for the thrift HTTP endpoint running.
   * This can change across Hive versions.
   */
  val THRIFT_HTTP_SERVICE_LIVE = "Started ThriftHttpCLIService in http"

  val SERVER_STARTUP_TIMEOUT = 3.minutes

  private def startThriftServer(attempt: Int) = {
    warehousePath = Utils.createTempDir()
    warehousePath.delete()
    metastorePath = Utils.createTempDir()
    metastorePath.delete()
    operationLogPath = Utils.createTempDir()
    operationLogPath.delete()
    lScratchDir = Utils.createTempDir()
    lScratchDir.delete()
    logPath = null
    logTailingProcess = null

    val command = serverStartCommand()

    diagnosisBuffer ++=
      s"""
         |### Attempt $attempt ###
         |HiveThriftServer2 command line: $command
         |Listening port: 0
         |System user: $user
       """.stripMargin.split("\n")

    logPath = {
      val lines = Utils.executeAndGetOutput(
        command = command,
        extraEnvironment = Map(
          // Disables SPARK_TESTING to exclude log4j.properties in test directories.
          "SPARK_TESTING" -> "0",
          // But set SPARK_SQL_TESTING to make spark-class happy.
          "SPARK_SQL_TESTING" -> "1",
          // Points SPARK_PID_DIR to SPARK_HOME, otherwise only 1 Thrift server instance can be
          // started at a time, which is not Jenkins friendly.
          "SPARK_PID_DIR" -> pidDir.getCanonicalPath),
        redirectStderr = true)

      logInfo(s"COMMAND: $command")
      logInfo(s"OUTPUT: $lines")
      lines.split("\n").collectFirst {
        case line if line.contains(LOG_FILE_MARK) => new File(line.drop(LOG_FILE_MARK.length))
      }.getOrElse {
        throw new RuntimeException("Failed to find HiveThriftServer2 log file.")
      }
    }

    val serverStarted = Promise[Unit]()

    // Ensures that the following "tail" command won't fail.
    logPath.createNewFile()
    val successLine = if (mode == ServerMode.http) {
      THRIFT_HTTP_SERVICE_LIVE
    } else {
      THRIFT_BINARY_SERVICE_LIVE
    }

    logTailingProcess = {
      val command = s"/usr/bin/env tail -n +0 -f ${logPath.getCanonicalPath}".split(" ")
      // Using "-n +0" to make sure all lines in the log file are checked.
      val builder = new ProcessBuilder(command: _*)
      val captureOutput = (line: String) => diagnosisBuffer.synchronized {
        diagnosisBuffer += line

        if (line.contains(successLine)) {
          listeningPort = line.split(" on port ")(1).split(' ').head.toInt
          logInfo(s"Started HiveThriftServer2: port=$listeningPort, mode=$mode, attempt=$attempt")
          serverStarted.trySuccess(())
          ()
        }
      }

      val process = builder.start()

      new ProcessOutputCapturer(process.getInputStream, captureOutput).start()
      new ProcessOutputCapturer(process.getErrorStream, captureOutput).start()
      process
    }

    ShutdownHookManager.addShutdownHook(stopThriftServer _)
    ThreadUtils.awaitResult(serverStarted.future, SERVER_STARTUP_TIMEOUT)
  }

  private def stopThriftServer(): Unit = {
    if (pidDir.list.nonEmpty) {
      // The `spark-daemon.sh' script uses kill, which is not synchronous, have to wait for a while.
      Utils.executeAndGetOutput(
        command = Seq(stopScript),
        extraEnvironment = Map("SPARK_PID_DIR" -> pidDir.getCanonicalPath))
      Thread.sleep(3.seconds.toMillis)

      warehousePath.delete()
      warehousePath = null

      metastorePath.delete()
      metastorePath = null

      operationLogPath.delete()
      operationLogPath = null

      lScratchDir.delete()
      lScratchDir = null

      Option(logPath).foreach(_.delete())
      logPath = null

      Option(logTailingProcess).foreach(_.destroy())
      logTailingProcess = null
    }
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

  override def beforeEach(): Unit = {
    super.beforeEach()
    System.gc()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    diagnosisBuffer.clear()

    // Retries up to 3 times with different port numbers if the server fails to start
    (1 to 3).foldLeft(Try(startThriftServer(0))) { case (started, attempt) =>
      started.orElse {
        stopThriftServer()
        Try {
          startThriftServer(attempt)
          eventually(timeout(30.seconds), interval(1.seconds)) {
            withJdbcStatement() { _.execute("SELECT 1") }
          }
        }
      }
    }.recover {
      case cause: Throwable =>
        dumpLogs()
        throw cause
    }.get

    logInfo(s"HiveThriftServer2 started successfully")
  }

  override protected def afterAll(): Unit = {
    try {
      stopThriftServer()
      logInfo("HiveThriftServer2 stopped")
    } finally {
      super.afterAll()
    }
  }

  Utils.classForName(classOf[HiveDriver].getCanonicalName)

  protected def jdbcUri(database: String = "default"): String = if (mode == ServerMode.http) {
    s"""jdbc:hive2://$localhost:$serverPort/
       |$database;
       |transportMode=http;
       |httpPath=cliservice;?
       |${hiveConfList}#${hiveVarList}
     """.stripMargin.split("\n").mkString.trim
  } else {
    s"jdbc:hive2://$localhost:$serverPort/$database?${hiveConfList}#${hiveVarList}"
  }

  private def tryCaptureSysLog(f: => Unit): Unit = {
    try f catch {
      case e: Exception =>
        // Dump the HiveThriftServer2 log if error occurs, e.g. getConnection failure.
        dumpLogs()
        throw e
    }
  }

  def withMultipleConnectionJdbcStatement(
      tableNames: String*)(fs: (Statement => Unit)*): Unit = tryCaptureSysLog {
    val user = System.getProperty("user.name")
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUri(), user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      tableNames.foreach { name =>
        // TODO: Need a better way to drop the view.
        if (name.toUpperCase(Locale.ROOT).startsWith("VIEW")) {
          statements(0).execute(s"DROP VIEW IF EXISTS $name")
        } else {
          statements(0).execute(s"DROP TABLE IF EXISTS $name")
        }
      }
      statements.foreach(_.close())
      connections.foreach(_.close())
    }
  }

  def withDatabase(dbNames: String*)(fs: (Statement => Unit)*): Unit = tryCaptureSysLog {
    val user = System.getProperty("user.name")
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUri(), user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      dbNames.foreach { name =>
        statements(0).execute(s"DROP DATABASE IF EXISTS $name")
      }
      statements.foreach(_.close())
      connections.foreach(_.close())
    }
  }

  def withJdbcStatement(tableNames: String*)(f: Statement => Unit): Unit = {
    withMultipleConnectionJdbcStatement(tableNames: _*)(f)
  }
}

/**
 * Common tests for both binary and http mode thrift server
 * TODO: SPARK-31914: Move common tests from subclasses to this trait
 */
abstract class HiveThriftServer2Test extends HiveThriftServer2TestBase {
  test("SPARK-17819: Support default database in connection URIs") {
    withDatabase("spark17819") { statement =>
      statement.execute(s"CREATE DATABASE IF NOT EXISTS spark17819")
      val jdbcStr = jdbcUri("spark17819")
      val connection = DriverManager.getConnection(jdbcStr, user, "")
      val statementN = connection.createStatement()
      try {
        val resultSet = statementN.executeQuery("select current_database()")
        resultSet.next()
        assert(resultSet.getString(1) === "spark17819")
      } finally {
        statementN.close()
        connection.close()
      }
    }
  }
}
