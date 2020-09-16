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

package org.apache.spark.sql.streaming.test

import java.io.File

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.{FakeV2Provider, InMemoryTableCatalog, InMemoryTableSessionCatalog}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION
import org.apache.spark.sql.streaming.StreamTest

class DataStreamWriterWithTableSuite extends StreamTest with BeforeAndAfter {
  import testImplicits._

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
    sqlContext.streams.active.foreach(_.stop())
  }

  test("write to table with custom catalog & no namespace") {
    val tableIdentifier = "testcat.table_name"

    spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING foo")
    checkAnswer(spark.table(tableIdentifier), Seq.empty)

    runTestWithStreamAppend(tableIdentifier)
  }

  test("write to table with custom catalog & namespace") {
    spark.sql("CREATE NAMESPACE testcat.ns")

    val tableIdentifier = "testcat.ns.table_name"

    spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING foo")
    checkAnswer(spark.table(tableIdentifier), Seq.empty)

    runTestWithStreamAppend(tableIdentifier)
  }

  test("write to table with default session catalog") {
    val v2Source = classOf[FakeV2Provider].getName
    spark.conf.set(V2_SESSION_CATALOG_IMPLEMENTATION.key,
      classOf[InMemoryTableSessionCatalog].getName)

    spark.sql("CREATE NAMESPACE ns")

    val tableIdentifier = "ns.table_name"
    spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING $v2Source")
    checkAnswer(spark.table(tableIdentifier), Seq.empty)

    runTestWithStreamAppend(tableIdentifier)
  }

  test("write to non-exist table with custom catalog") {
    val tableIdentifier = "testcat.nonexisttable"
    val existTableIdentifier = "testcat.ns.nonexisttable"

    spark.sql("CREATE NAMESPACE testcat.ns")
    spark.sql(s"CREATE TABLE $existTableIdentifier (id bigint, data string) USING foo")

    withTempDir { checkpointDir =>
      val exc = intercept[NoSuchTableException] {
        runStreamQueryAppendMode(tableIdentifier, checkpointDir, Seq.empty, Seq.empty)
      }
      assert(exc.getMessage.contains("nonexisttable"))
    }
  }

  test("write to file provider based table isn't allowed yet") {
    val tableIdentifier = "table_name"

    spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING parquet")
    checkAnswer(spark.table(tableIdentifier), Seq.empty)

    withTempDir { checkpointDir =>
      val exc = intercept[AnalysisException] {
        runStreamQueryAppendMode(tableIdentifier, checkpointDir, Seq.empty, Seq.empty)
      }
      assert(exc.getMessage.contains("doesn't support streaming write"))
    }
  }

  test("write to temporary view isn't allowed yet") {
    val tableIdentifier = "testcat.table_name"
    val tempViewIdentifier = "temp_view"

    spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING foo")
    checkAnswer(spark.table(tableIdentifier), Seq.empty)

    spark.table(tableIdentifier).createOrReplaceTempView(tempViewIdentifier)

    withTempDir { checkpointDir =>
      val exc = intercept[AnalysisException] {
        runStreamQueryAppendMode(tempViewIdentifier, checkpointDir, Seq.empty, Seq.empty)
      }
      assert(exc.getMessage.contains("doesn't support streaming write"))
    }
  }

  test("write to view shouldn't be allowed") {
    val tableIdentifier = "testcat.table_name"
    val viewIdentifier = "table_view"

    spark.sql(s"CREATE TABLE $tableIdentifier (id bigint, data string) USING foo")
    checkAnswer(spark.table(tableIdentifier), Seq.empty)

    spark.sql(s"CREATE VIEW $viewIdentifier AS SELECT id, data FROM $tableIdentifier")

    withTempDir { checkpointDir =>
      val exc = intercept[AnalysisException] {
        runStreamQueryAppendMode(viewIdentifier, checkpointDir, Seq.empty, Seq.empty)
      }
      assert(exc.getMessage.contains("doesn't support streaming write"))
    }
  }

  private def runTestWithStreamAppend(tableIdentifier: String) = {
    withTempDir { checkpointDir =>
      val input1 = Seq((1L, "a"), (2L, "b"), (3L, "c"))
      verifyStreamAppend(tableIdentifier, checkpointDir, Seq.empty, input1, input1)

      val input2 = Seq((4L, "d"), (5L, "e"), (6L, "f"))
      verifyStreamAppend(tableIdentifier, checkpointDir, Seq(input1), input2, input1 ++ input2)
    }
  }

  private def runStreamQueryAppendMode(
      tableIdentifier: String,
      checkpointDir: File,
      prevInputs: Seq[Seq[(Long, String)]],
      newInputs: Seq[(Long, String)]): Unit = {
    val inputData = MemoryStream[(Long, String)]
    val inputDF = inputData.toDF().toDF("id", "data")

    prevInputs.foreach { inputsPerBatch =>
      inputData.addData(inputsPerBatch: _*)
    }

    val query = inputDF
      .writeStream
      .table(tableIdentifier)
      .option("checkpointLocation", checkpointDir.getAbsolutePath)
      .start()

    inputData.addData(newInputs: _*)

    query.processAllAvailable()
    query.stop()
  }

  private def verifyStreamAppend(
      tableIdentifier: String,
      checkpointDir: File,
      prevInputs: Seq[Seq[(Long, String)]],
      newInputs: Seq[(Long, String)],
      expectedOutputs: Seq[(Long, String)]): Unit = {
    runStreamQueryAppendMode(tableIdentifier, checkpointDir, prevInputs, newInputs)
    checkAnswer(
      spark.table(tableIdentifier),
      expectedOutputs.map { case (id, data) => Row(id, data) }
    )
  }
}
