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

package org.apache.spark.sql.execution.streaming

import scala.language.implicitConversions

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.sources._
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.Utils

class MemorySinkSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("directly add data in Append output mode") {
    implicit val schema = new StructType().add(new StructField("value", IntegerType))
    val sink = new MemorySink
    val addBatch = addBatchFunc(sink, false) _

    // Before adding data, check output
    assert(sink.latestBatchId === None)
    checkAnswer(sink.latestBatchData, Seq.empty)
    checkAnswer(sink.allData, Seq.empty)

    // Add batch 0 and check outputs
    addBatch(0, 1 to 3)
    assert(sink.latestBatchId === Some(0))
    checkAnswer(sink.latestBatchData, 1 to 3)
    checkAnswer(sink.allData, 1 to 3)

    // Add batch 1 and check outputs
    addBatch(1, 4 to 6)
    assert(sink.latestBatchId === Some(1))
    checkAnswer(sink.latestBatchData, 4 to 6)
    checkAnswer(sink.allData, 1 to 6)     // new data should get appended to old data

    // Re-add batch 1 with different data, should not be added and outputs should not be changed
    addBatch(1, 7 to 9)
    assert(sink.latestBatchId === Some(1))
    checkAnswer(sink.latestBatchData, 4 to 6)
    checkAnswer(sink.allData, 1 to 6)

    // Add batch 2 and check outputs
    addBatch(2, 7 to 9)
    assert(sink.latestBatchId === Some(2))
    checkAnswer(sink.latestBatchData, 7 to 9)
    checkAnswer(sink.allData, 1 to 9)
  }

  test("directly add data in Update output mode") {
    implicit val schema = new StructType().add(new StructField("value", IntegerType))
    val sink = new MemorySink
    val addBatch = addBatchFunc(sink, false) _

    // Before adding data, check output
    assert(sink.latestBatchId === None)
    checkAnswer(sink.latestBatchData, Seq.empty)
    checkAnswer(sink.allData, Seq.empty)

    // Add batch 0 and check outputs
    addBatch(0, 1 to 3)
    assert(sink.latestBatchId === Some(0))
    checkAnswer(sink.latestBatchData, 1 to 3)
    checkAnswer(sink.allData, 1 to 3)

    // Add batch 1 and check outputs
    addBatch(1, 4 to 6)
    assert(sink.latestBatchId === Some(1))
    checkAnswer(sink.latestBatchData, 4 to 6)
    checkAnswer(sink.allData, 1 to 6) // new data should get appended to old data

    // Re-add batch 1 with different data, should not be added and outputs should not be changed
    addBatch(1, 7 to 9)
    assert(sink.latestBatchId === Some(1))
    checkAnswer(sink.latestBatchData, 4 to 6)
    checkAnswer(sink.allData, 1 to 6)

    // Add batch 2 and check outputs
    addBatch(2, 7 to 9)
    assert(sink.latestBatchId === Some(2))
    checkAnswer(sink.latestBatchData, 7 to 9)
    checkAnswer(sink.allData, 1 to 9)
  }

  test("directly add data in Complete output mode") {
    implicit val schema = new StructType().add(new StructField("value", IntegerType))
    val sink = new MemorySink
    val addBatch = addBatchFunc(sink, true) _

    // Before adding data, check output
    assert(sink.latestBatchId === None)
    checkAnswer(sink.latestBatchData, Seq.empty)
    checkAnswer(sink.allData, Seq.empty)

    // Add batch 0 and check outputs
    addBatch(0, 1 to 3)
    assert(sink.latestBatchId === Some(0))
    checkAnswer(sink.latestBatchData, 1 to 3)
    checkAnswer(sink.allData, 1 to 3)

    // Add batch 1 and check outputs
    addBatch(1, 4 to 6)
    assert(sink.latestBatchId === Some(1))
    checkAnswer(sink.latestBatchData, 4 to 6)
    checkAnswer(sink.allData, 4 to 6)     // new data should replace old data

    // Re-add batch 1 with different data, should not be added and outputs should not be changed
    addBatch(1, 7 to 9)
    assert(sink.latestBatchId === Some(1))
    checkAnswer(sink.latestBatchData, 4 to 6)
    checkAnswer(sink.allData, 4 to 6)

    // Add batch 2 and check outputs
    addBatch(2, 7 to 9)
    assert(sink.latestBatchId === Some(2))
    checkAnswer(sink.latestBatchData, 7 to 9)
    checkAnswer(sink.allData, 7 to 9)
  }


  test("registering as a table in Append output mode") {
    val input = MemoryStream[Int]
    val query = input.toDF().writeStream
      .format("memory")
      .outputMode("append")
      .queryName("memStream")
      .start()
    input.addData(1, 2, 3)
    query.processAllAvailable()

    checkDataset(
      spark.table("memStream").as[Int],
      1, 2, 3)

    input.addData(4, 5, 6)
    query.processAllAvailable()
    checkDataset(
      spark.table("memStream").as[Int],
      1, 2, 3, 4, 5, 6)

    query.stop()
  }

  test("registering as a table in Complete output mode") {
    val input = MemoryStream[Int]
    val query = input.toDF()
      .groupBy("value")
      .count()
      .writeStream
      .format("memory")
      .outputMode("complete")
      .queryName("memStream")
      .start()
    input.addData(1, 2, 3)
    query.processAllAvailable()

    checkDatasetUnorderly(
      spark.table("memStream").as[(Int, Long)],
      (1, 1L), (2, 1L), (3, 1L))

    input.addData(4, 5, 6)
    query.processAllAvailable()
    checkDatasetUnorderly(
      spark.table("memStream").as[(Int, Long)],
      (1, 1L), (2, 1L), (3, 1L), (4, 1L), (5, 1L), (6, 1L))

    query.stop()
  }

  test("registering as a table in Update output mode") {
    val input = MemoryStream[Int]
    val query = input.toDF().writeStream
      .format("memory")
      .outputMode("update")
      .queryName("memStream")
      .start()
    input.addData(1, 2, 3)
    query.processAllAvailable()

    checkDataset(
      spark.table("memStream").as[Int],
      1, 2, 3)

    input.addData(4, 5, 6)
    query.processAllAvailable()
    checkDataset(
      spark.table("memStream").as[Int],
      1, 2, 3, 4, 5, 6)

    query.stop()
  }

  test("MemoryPlan statistics") {
    implicit val schema = new StructType().add(new StructField("value", IntegerType))
    val sink = new MemorySink
    val addBatch = addBatchFunc(sink, false) _
    val plan = new MemoryPlan(sink, schema.toAttributes)

    // Before adding data, check output
    checkAnswer(sink.allData, Seq.empty)
    assert(plan.stats.sizeInBytes === 0)

    addBatch(0, 1 to 3)
    plan.invalidateStatsCache()
    assert(plan.stats.sizeInBytes === 36)

    addBatch(1, 4 to 6)
    plan.invalidateStatsCache()
    assert(plan.stats.sizeInBytes === 72)
  }

  ignore("stress test") {
    // Ignore the stress test as it takes several minutes to run
    (0 until 1000).foreach { _ =>
      val input = MemoryStream[Int]
      val query = input.toDF().writeStream
        .format("memory")
        .queryName("memStream")
        .start()
      input.addData(1, 2, 3)
      query.processAllAvailable()

      checkDataset(
        spark.table("memStream").as[Int],
        1, 2, 3)

      input.addData(4, 5, 6)
      query.processAllAvailable()
      checkDataset(
        spark.table("memStream").as[Int],
        1, 2, 3, 4, 5, 6)

      query.stop()
    }
  }

  test("error when no name is specified") {
    val error = intercept[AnalysisException] {
      val input = MemoryStream[Int]
      val query = input.toDF().writeStream
          .format("memory")
          .start()
    }

    assert(error.message contains "queryName must be specified")
  }

  test("error if attempting to resume specific checkpoint") {
    val location = Utils.createTempDir(namePrefix = "steaming.checkpoint").getCanonicalPath

    val input = MemoryStream[Int]
    val query = input.toDF().writeStream
        .format("memory")
        .queryName("memStream")
        .option("checkpointLocation", location)
        .start()
    input.addData(1, 2, 3)
    query.processAllAvailable()
    query.stop()

    intercept[AnalysisException] {
      input.toDF().writeStream
        .format("memory")
        .queryName("memStream")
        .option("checkpointLocation", location)
        .start()
    }
  }

  test("data writer") {
    val partition = 1234
    val writer = new MemoryDataWriter(
      partition, new StructType().add("i", "int"))
    writer.write(InternalRow(1))
    writer.write(InternalRow(2))
    writer.write(InternalRow(44))
    val msg = writer.commit()
    assert(msg.data.map(_.getInt(0)) == Seq(1, 2, 44))
    assert(msg.partition == partition)

    // Buffer should be cleared, so repeated commits should give empty.
    assert(writer.commit().data.isEmpty)
  }

  test("streaming writer") {
    val sink = new MemorySink
    val write = new MemoryStreamingWrite(
      sink, new StructType().add("i", "int"), needTruncate = false)
    write.commit(0,
      Array(
        MemoryWriterCommitMessage(0, Seq(Row(1), Row(2))),
        MemoryWriterCommitMessage(1, Seq(Row(3), Row(4))),
        MemoryWriterCommitMessage(2, Seq(Row(6), Row(7)))
      ))
    assert(sink.latestBatchId.contains(0))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(1, 2, 3, 4, 6, 7))
    write.commit(19,
      Array(
        MemoryWriterCommitMessage(3, Seq(Row(11), Row(22))),
        MemoryWriterCommitMessage(0, Seq(Row(33)))
      ))
    assert(sink.latestBatchId.contains(19))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(11, 22, 33))

    assert(sink.allData.map(_.getInt(0)).sorted == Seq(1, 2, 3, 4, 6, 7, 11, 22, 33))
  }

  private def addBatchFunc(sink: MemorySink, needTruncate: Boolean)(
      batchId: Long,
      vals: Seq[Int]): Unit = {
    sink.write(batchId, needTruncate, vals.map(Row(_)).toArray)
  }

  private def checkAnswer(rows: Seq[Row], expected: Seq[Int])(implicit schema: StructType): Unit = {
    checkAnswer(
      sqlContext.createDataFrame(sparkContext.makeRDD(rows), schema),
      intsToDF(expected)(schema))
  }

  private implicit def intsToDF(seq: Seq[Int])(implicit schema: StructType): DataFrame = {
    require(schema.fields.size === 1)
    sqlContext.createDataset(seq).toDF(schema.fieldNames.head)
  }
}
