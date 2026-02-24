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

package org.apache.spark.sql.execution.python

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.{PythonWorkerLogBlockId, PythonWorkerLogBlockIdGenerator, PythonWorkerLogLine}
import org.apache.spark.util.LogUtils

class PythonWorkerLogsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.PYTHON_WORKER_LOGGING_ENABLED.key, "true")

  protected override def afterEach(): Unit = {
    try {
      val blockManager = spark.sparkContext.env.blockManager
      blockManager.getMatchingBlockIds(_.isInstanceOf[PythonWorkerLogBlockId])
        .foreach(blockManager.removeBlock(_))
    } finally {
      super.afterEach()
    }
  }

  test("fail if logging is not enabled") {
    withSQLConf(SQLConf.PYTHON_WORKER_LOGGING_ENABLED.key -> "false") {
      val ex = intercept[AnalysisException] {
        spark.tvf.python_worker_logs().collect()
      }
      checkError(
        exception = ex,
        condition = "FEATURE_NOT_ENABLED",
        parameters = Map(
          "featureName" -> "Python Worker Logging",
          "configKey" -> "spark.sql.pyspark.worker.logging.enabled",
          "configValue" -> "true"
        )
      )
    }
  }

  test("schema") {
    val schema = spark.tvf.python_worker_logs().schema
    assert(schema == StructType.fromDDL(LogUtils.SPARK_LOG_SCHEMA))
  }

  private def prepareLogs(sessionId: String): Unit = {
    val blockManager = spark.sparkContext.env.blockManager
    val logBlockWriter = blockManager.getRollingLogWriter(
      new PythonWorkerLogBlockIdGenerator(sessionId, "1234"))
    logBlockWriter.writeLog(
      PythonWorkerLogLine(0L, 1L, """{"level":"INFO","msg":"msg1"}"""))
    logBlockWriter.writeLog(
      PythonWorkerLogLine(1L, 2L, """{"level":"ERROR","msg":"msg2"}"""))
    logBlockWriter.close()
  }

  test("read logs") {
    prepareLogs(spark.sessionUUID)

    val df = spark.tvf.python_worker_logs()
    assert(df.count() == 2)
    checkAnswer(
      df.select($"level", $"msg"),
      Seq(Row("INFO", "msg1"), Row("ERROR", "msg2")))
  }

  test("can't read logs for another session") {
    prepareLogs(UUID.randomUUID.toString)

    val df = spark.tvf.python_worker_logs()
    assert(df.count() == 0)
  }
}
