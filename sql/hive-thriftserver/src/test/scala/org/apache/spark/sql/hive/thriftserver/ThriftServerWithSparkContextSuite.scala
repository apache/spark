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

import java.sql.SQLException
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.hive.service.cli.{HiveSQLException, OperationHandle}

import org.apache.spark.TaskKilled
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.internal.SQLConf

trait ThriftServerWithSparkContextSuite extends SharedThriftServer {

  test("the scratch dir will not be exist") {
    assert(!tempScratchDir.exists())
  }

  test("SPARK-29911: Uncache cached tables when session closed") {
    val cacheManager = spark.sharedState.cacheManager
    val globalTempDB = spark.sharedState.globalTempViewManager.database
    withJdbcStatement { statement =>
      statement.execute("CACHE TABLE tempTbl AS SELECT 1")
    }
    // the cached data of local temporary view should be uncached
    assert(cacheManager.isEmpty)
    try {
      withJdbcStatement { statement =>
        statement.execute("CREATE GLOBAL TEMP VIEW globalTempTbl AS SELECT 1, 2")
        statement.execute(s"CACHE TABLE $globalTempDB.globalTempTbl")
      }
      // the cached data of global temporary view shouldn't be uncached
      assert(!cacheManager.isEmpty)
    } finally {
      withJdbcStatement { statement =>
        statement.execute(s"UNCACHE TABLE IF EXISTS $globalTempDB.globalTempTbl")
      }
      assert(cacheManager.isEmpty)
    }
  }

  test("Full stack traces as error message for jdbc or thrift client") {
    val sql = "select from_json('a', 'a INT', map('mode', 'FAILFAST'))"
    withCLIServiceClient() { client =>
      val sessionHandle = client.openSession(user, "")

      val confOverlay = new java.util.HashMap[java.lang.String, java.lang.String]
      val e = intercept[HiveSQLException] {
        client.executeStatement(
          sessionHandle,
          sql,
          confOverlay)
      }
      assert(e.getMessage.contains("JsonParseException: Unrecognized token 'a'"))
      assert(!e.getMessage.contains(
        "SparkException: Malformed records are detected in record parsing"))
    }

    withJdbcStatement { statement =>
      val e = intercept[SQLException] {
        statement.executeQuery(sql)
      }
      assert(e.getMessage.contains("JsonParseException: Unrecognized token 'a'"))
      assert(e.getMessage.contains(
        "SparkException: Malformed records are detected in record parsing"))
    }
  }

  test("SPARK-33526: Add config to control if cancel invoke interrupt task on thriftserver") {
    withJdbcStatement { statement =>
      val forceCancel = new AtomicBoolean(false)
      val listener = new SparkListener {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          assert(taskEnd.reason.isInstanceOf[TaskKilled])
          if (forceCancel.get()) {
            assert(System.currentTimeMillis() - taskEnd.taskInfo.launchTime < 1000)
          } else {
            // avoid accuracy, we check 2s instead of 3s.
            assert(System.currentTimeMillis() - taskEnd.taskInfo.launchTime >= 2000)
          }
        }
      }

      spark.sparkContext.addSparkListener(listener)
      try {
        Seq(true, false).foreach { force =>
          statement.setQueryTimeout(0)
          statement.execute(s"SET ${SQLConf.THRIFTSERVER_FORCE_CANCEL.key}=$force")
          statement.setQueryTimeout(1)
          forceCancel.set(force)
          val e = intercept[SQLException] {
            statement.execute("select java_method('java.lang.Thread', 'sleep', 3000L)")
          }.getMessage
          assert(e.contains("Query timed out"))
        }
      } finally {
        spark.sparkContext.removeSparkListener(listener)
      }
    }
  }

  test("SPARK-21957: get current_user through thrift server") {
    val clientUser = "storm_earth_fire_heed_my_call"
    val sql = "select current_user()"

    withCLIServiceClient(clientUser) { client =>
      val sessionHandle = client.openSession(clientUser, "")
      val confOverlay = new java.util.HashMap[java.lang.String, java.lang.String]
      val exec: String => OperationHandle = client.executeStatement(sessionHandle, _, confOverlay)

      exec(s"set ${SQLConf.ANSI_ENABLED.key}=false")

      val userFuncs = Seq("user", "current_user")
      userFuncs.foreach { func =>
        val opHandle1 = exec(s"select $func(), $func")
        val rowSet1 = client.fetchResults(opHandle1)
        rowSet1.getColumns.forEach { col =>
          assert(col.getStringVal.getValues.get(0) === clientUser)
        }
      }

      exec(s"set ${SQLConf.ANSI_ENABLED.key}=true")
      exec(s"set ${SQLConf.ENFORCE_RESERVED_KEYWORDS.key}=true")
      userFuncs.foreach { func =>
        val opHandle2 = exec(s"select $func")
        assert(client.fetchResults(opHandle2)
          .getColumns.get(0).getStringVal.getValues.get(0) === clientUser)
      }

      userFuncs.foreach { func =>
        val e = intercept[HiveSQLException](exec(s"select $func()"))
        assert(e.getMessage.contains(func))
      }
    }
  }
}

class ThriftServerWithSparkContextInBinarySuite extends ThriftServerWithSparkContextSuite {
  override def mode: ServerMode.Value = ServerMode.binary
}

class ThriftServerWithSparkContextInHttpSuite extends ThriftServerWithSparkContextSuite {
  override def mode: ServerMode.Value = ServerMode.http
}
