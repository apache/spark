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

package org.apache.spark.sql.streaming

import java.io.File

import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.must.Matchers

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.InMemoryTableCatalog
import org.apache.spark.sql.execution.streaming.MemoryStream

class DataStreamWriterV2Suite extends StreamTest with BeforeAndAfter with Matchers with Logging {
  import testImplicits._

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
    sqlContext.streams.active.foreach(_.stop())
  }

  test("Append: basic") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string) USING foo")

    checkAnswer(spark.table("testcat.table_name"), Seq.empty)

    def verifyStreamAppend(
        checkpointDir: File,
        prevInputs: Seq[Seq[(Long, String)]],
        newInputs: Seq[(Long, String)],
        expectedOutputs: Seq[(Long, String)]): Unit = {
      runStreamQueryAppendMode(checkpointDir, prevInputs, newInputs)
      checkAnswer(
        spark.table("testcat.table_name"),
        expectedOutputs.map { case (id, data) => Row(id, data) }
      )
    }

    withTempDir { checkpointDir =>
      val input1 = Seq((1L, "a"), (2L, "b"), (3L, "c"))
      verifyStreamAppend(checkpointDir, Seq.empty, input1, input1)

      val input2 = Seq((4L, "d"), (5L, "e"), (6L, "f"))
      verifyStreamAppend(checkpointDir, Seq(input1), input2, input1 ++ input2)
    }
  }

  test("Append: fail if table does not exist") {
    withTempDir { checkpointDir =>
      val exc = intercept[NoSuchTableException] {
        runStreamQueryAppendMode(checkpointDir, Seq.empty, Seq.empty)
      }
      assert(exc.getMessage.contains("table_name"))
    }
  }

  private def runStreamQueryAppendMode(
      checkpointDir: File,
      prevInputs: Seq[Seq[(Long, String)]],
      newInputs: Seq[(Long, String)]): Unit = {
    val inputData = MemoryStream[(Long, String)]
    val inputDF = inputData.toDF().toDF("id", "data")

    prevInputs.foreach { inputsPerBatch =>
      inputData.addData(inputsPerBatch: _*)
    }

    val query = inputDF
      .writeStreamTo("testcat.table_name")
      .checkpointLocation(checkpointDir.getAbsolutePath)
      .append()

    inputData.addData(newInputs: _*)

    query.processAllAvailable()
    query.stop()
  }

  test("TruncateAndAppend (complete mode): basic") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, sum bigint) USING foo")

    checkAnswer(spark.table("testcat.table_name"), Seq.empty)

    def verifyStreamTruncateAndAppend(
        checkpointDir: File,
        prevInputs: Seq[Seq[(Long, Long)]],
        newInputs: Seq[(Long, Long)],
        expectedOutputs: Seq[(Long, Long)]): Unit = {
      runStreamQueryCompleteMode(checkpointDir, prevInputs, newInputs)
      checkAnswer(
        spark.table("testcat.table_name"),
        expectedOutputs.map { case (id, data) => Row(id, data) }
      )
    }

    withTempDir { checkpointDir =>
      val input1 = Seq((1L, 1L), (2L, 2L), (3L, 3L))
      verifyStreamTruncateAndAppend(checkpointDir, Seq.empty, input1, Seq((0L, 2L), (1L, 4L)))

      val input2 = Seq((4L, 4L), (5L, 5L), (6L, 6L))
      verifyStreamTruncateAndAppend(checkpointDir, Seq(input1), input2, Seq((0L, 12L), (1L, 9L)))
    }
  }

  test("TruncateAndAppend (complete mode): fail if table does not exist") {
    val inputData = MemoryStream[(Long, String)]
    val inputDF = inputData.toDF().toDF("id", "data")

    withTempDir { checkpointDir =>
      val exc = intercept[NoSuchTableException] {
        runStreamQueryCompleteMode(checkpointDir, Seq.empty, Seq.empty)
      }
      assert(exc.getMessage.contains("table_name"))
    }
  }

  private def runStreamQueryCompleteMode(
      checkpointDir: File,
      prevInputs: Seq[Seq[(Long, Long)]],
      newInputs: Seq[(Long, Long)]): Unit = {
    val inputData = MemoryStream[(Long, Long)]
    val inputDF = inputData.toDF().toDF("id", "value")

    prevInputs.foreach { inputsPerBatch =>
      inputData.addData(inputsPerBatch: _*)
    }

    val query = inputDF
      .selectExpr("id % 2 AS key", "value")
      .groupBy("key")
      .agg("value" -> "sum")
      .writeStreamTo("testcat.table_name")
      .checkpointLocation(checkpointDir.getAbsolutePath)
      .truncateAndAppend()

    inputData.addData(newInputs: _*)

    query.processAllAvailable()
    query.stop()
  }
}
