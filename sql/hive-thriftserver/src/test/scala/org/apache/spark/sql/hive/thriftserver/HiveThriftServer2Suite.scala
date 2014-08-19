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

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

import java.io.{BufferedReader, InputStreamReader}
import java.net.ServerSocket
import java.sql.{Connection, DriverManager, Statement}

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.util.getTempFilePath

/**
 * Test for the HiveThriftServer2 using JDBC.
 */
class HiveThriftServer2Suite extends FunSuite with BeforeAndAfterAll with TestUtils with Logging {

  val WAREHOUSE_PATH = getTempFilePath("warehouse")
  val METASTORE_PATH = getTempFilePath("metastore")

  val DRIVER_NAME  = "org.apache.hive.jdbc.HiveDriver"
  val TABLE = "test"
  val HOST = "localhost"
  val PORT =  {
    // Let the system to choose a random available port to avoid collision with other parallel
    // builds.
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  Class.forName(DRIVER_NAME)

  override def beforeAll() { launchServer() }

  override def afterAll() { stopServer() }

  private def launchServer(args: Seq[String] = Seq.empty) {
    // Forking a new process to start the Hive Thrift server. The reason to do this is it is
    // hard to clean up Hive resources entirely, so we just start a new process and kill
    // that process for cleanup.
    val jdbcUrl = s"jdbc:derby:;databaseName=$METASTORE_PATH;create=true"
    val command =
      s"""../../sbin/start-thriftserver.sh
         |  --master local
         |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$jdbcUrl
         |  --hiveconf ${ConfVars.METASTOREWAREHOUSE}=$METASTORE_PATH
         |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST}=$HOST
         |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_PORT}=$PORT
       """.stripMargin.split("\\s+")

    val pb = new ProcessBuilder(command ++ args: _*)
    val environment = pb.environment()
    process = pb.start()
    inputReader = new BufferedReader(new InputStreamReader(process.getInputStream))
    errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream))
    waitForOutput(inputReader, "ThriftBinaryCLIService listening on", 300000)

    // Spawn a thread to read the output from the forked process.
    // Note that this is necessary since in some configurations, log4j could be blocked
    // if its output to stderr are not read, and eventually blocking the entire test suite.
    future {
      while (true) {
        val stdout = readFrom(inputReader)
        val stderr = readFrom(errorReader)
        print(stdout)
        print(stderr)
        Thread.sleep(50)
      }
    }
  }

  private def stopServer() {
    process.destroy()
    process.waitFor()
  }

  test("test query execution against a Hive Thrift server") {
    Thread.sleep(5 * 1000)
    val dataFilePath = getDataFile("data/files/small_kv.txt")
    val stmt = createStatement()
    stmt.execute("DROP TABLE IF EXISTS test")
    stmt.execute("DROP TABLE IF EXISTS test_cached")
    stmt.execute("CREATE TABLE test(key INT, val STRING)")
    stmt.execute(s"LOAD DATA LOCAL INPATH '$dataFilePath' OVERWRITE INTO TABLE test")
    stmt.execute("CREATE TABLE test_cached AS SELECT * FROM test LIMIT 4")
    stmt.execute("CACHE TABLE test_cached")

    var rs = stmt.executeQuery("SELECT COUNT(*) FROM test")
    rs.next()
    assert(rs.getInt(1) === 5)

    rs = stmt.executeQuery("SELECT COUNT(*) FROM test_cached")
    rs.next()
    assert(rs.getInt(1) === 4)

    stmt.close()
  }

  test("SPARK-3004 regression: result set containing NULL") {
    Thread.sleep(5 * 1000)
    val dataFilePath = getDataFile("data/files/small_kv_with_null.txt")
    val stmt = createStatement()
    stmt.execute("DROP TABLE IF EXISTS test_null")
    stmt.execute("CREATE TABLE test_null(key INT, val STRING)")
    stmt.execute(s"LOAD DATA LOCAL INPATH '$dataFilePath' OVERWRITE INTO TABLE test_null")

    val rs = stmt.executeQuery("SELECT * FROM test_null WHERE key IS NULL")
    var count = 0
    while (rs.next()) {
      count += 1
    }
    assert(count === 5)

    stmt.close()
  }

  def getConnection: Connection = {
    val connectURI = s"jdbc:hive2://localhost:$PORT/"
    DriverManager.getConnection(connectURI, System.getProperty("user.name"), "")
  }

  def createStatement(): Statement = getConnection.createStatement()
}
