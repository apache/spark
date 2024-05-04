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

package org.apache.spark.sql

import java.io.File

import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.LogUtils.LOG_SCHEMA

/**
 * Test suite for querying Spark logs using SQL.
 */
class LogQuerySuite extends QueryTest with SharedSparkSession with Logging {

  val logFile: File = {
    val pwd = new File(".").getCanonicalPath
    new File(pwd + "/target/LogQuerySuite.log")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    // Clear the log file
    if (logFile.exists()) {
      logFile.delete()
    }
  }

  private def createTempView(viewName: String): Unit = {
    spark.read.schema(LOG_SCHEMA).json(logFile.getCanonicalPath).createOrReplaceTempView(viewName)
  }

  test("Query Spark logs using SQL") {
    val msg = log"Lost executor ${MDC(LogKeys.EXECUTOR_ID, "1")}."
    logError(msg)

    withTempView("logs") {
      createTempView("logs")
      checkAnswer(
        spark.sql(s"SELECT level, msg, context, exception FROM logs WHERE msg = '${msg.message}'"),
        Row("ERROR", msg.message, Map(LogKeys.EXECUTOR_ID.name -> "1"), null) :: Nil)
    }
  }

  test("Query Spark logs with exception using SQL") {
    val msg = log"Task ${MDC(LogKeys.TASK_ID, "2")} failed."
    val exception = new RuntimeException("OOM")
    logError(msg, exception)

    withTempView("logs") {
      createTempView("logs")
      val expectedMDC = Map(LogKeys.TASK_ID.name -> "2")
      checkAnswer(
        spark.sql("SELECT level, msg, context, exception.class, exception.msg FROM logs " +
          s"WHERE msg = '${msg.message}'"),
        Row("ERROR", msg.message, expectedMDC, "java.lang.RuntimeException", "OOM") :: Nil)

      val stacktrace =
        spark.sql(s"SELECT exception.stacktrace FROM logs WHERE msg = '${msg.message}'").collect()
      assert(stacktrace.length == 1)
      val topStacktraceArray = stacktrace.head.getSeq[Row](0).head
      assert(topStacktraceArray.getString(0) == this.getClass.getName)
      assert(topStacktraceArray.getString(1) != "")
      assert(topStacktraceArray.getString(2) == this.getClass.getSimpleName + ".scala")
      assert(topStacktraceArray.getString(3) != "")
    }
  }
}
