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
import java.sql.SQLException
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.hive.service.cli.{FetchOrientation, FetchType, GetInfoType, RowSet}
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.HiveTestJars
import org.apache.spark.sql.internal.StaticSQLConf.HIVE_THRIFT_SERVER_SINGLESESSION
import org.apache.spark.util.Utils

object TestData {
  def getTestDataFilePath(name: String): URL = {
    Thread.currentThread().getContextClassLoader.getResource(s"data/files/$name")
  }

  val smallKv = getTestDataFilePath("small_kv.txt")
  val smallKvWithNull = getTestDataFilePath("small_kv_with_null.txt")
}

class HiveThriftBinaryServerSuite extends JdbcConnectionSuite {

  private def withCLIServiceClient(f: ThriftCLIServiceClient => Unit): Unit = {
    // Transport creation logic below mimics HiveConnection.createBinaryTransport
    val rawTransport = new TSocket("localhost", serverPort)
    val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
    val protocol = new TBinaryProtocol(transport)
    val client = new ThriftCLIServiceClient(new ThriftserverShimUtils.Client(protocol))

    transport.open()
    try f(client) finally transport.close()
  }

  test("GetInfo Thrift API") {
    withCLIServiceClient { client =>
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

          rows_next.numRows()
        }

        // Fetch result second time from first row
        assertResult(5, "Repeat fetching result from first row") {

          val rows_first = client.fetchResults(
            operationHandle,
            FetchOrientation.FETCH_FIRST,
            1000,
            FetchType.QUERY_OUTPUT)

          rows_first.numRows()
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

  test("SPARK-11043 check operation log root directory") {
    assert(operationLogPath.exists())
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
      val sessionHandle = client.openSession(user, "")
      val sessionID = sessionHandle.getSessionId

      if (HiveUtils.isHive23) {
        assert(pipeoutFileList(sessionID).length == 2)
      } else {
        assert(pipeoutFileList(sessionID).length == 1)
      }

      client.closeSession(sessionHandle)

      assert(pipeoutFileList(sessionID).length == 0)
    }
  }

  test("ThriftCLIService FetchResults FETCH_FIRST, FETCH_NEXT, FETCH_PRIOR") {
    def checkResult(rows: RowSet, start: Long, end: Long): Unit = {
      assert(rows.getStartOffset() == start)
      assert(rows.numRows() == end - start)
      rows.iterator.asScala.zip((start until end).iterator).foreach { case (row, v) =>
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
      var rows: RowSet = null

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
          rows_next.numRows()
        }
      }
    }
  }
}

class HiveThriftHttpServerSuite extends JdbcConnectionSuite {
  override def mode: ServerMode.Value = ServerMode.http
}

class SingleSessionSuite extends SharedThriftServer {
  override def sparkConf: SparkConf = {
    super.sparkConf
      .set(HIVE_THRIFT_SERVER_SINGLESESSION, true)
  }

  test("share the temporary functions across JDBC connections") {
    withMultipleConnectionJdbcStatement()(
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

class HiveThriftCleanUpScratchDirSuite extends SharedThriftServer {
  var tempScratchDir: File = _

  override def beforeAll(): Unit = {
    tempScratchDir = Utils.createTempDir()
    tempScratchDir.setWritable(true, false)
    assert(tempScratchDir.list().isEmpty)
    new File(tempScratchDir.getAbsolutePath + File.separator + "SPARK-31626").createNewFile()
    assert(tempScratchDir.list().nonEmpty)
    super.beforeAll()
  }

  override def extraConf: Map[String, String] = Map(
    ConfVars.HIVE_START_CLEANUP_SCRATCHDIR.varname -> "true",
    ConfVars.SCRATCHDIR.varname -> tempScratchDir.getAbsolutePath)

  // TODO restore this with old start-thriftserver script
  ignore("Cleanup the Hive scratchdir when starting the Hive Server") {
    assert(!tempScratchDir.exists())
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT id FROM range(1)")
      assert(rs.next())
      assert(rs.getLong(1) === 0L)
    }
  }

  override def afterAll(): Unit = {
    Utils.deleteRecursively(tempScratchDir)
    super.afterAll()
  }
}

object ServerMode extends Enumeration {
  val binary, http = Value
}
