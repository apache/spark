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

import scala.collection.JavaConverters._

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.sources._
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.streaming.{OutputMode, StreamTest}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType

class MemorySinkV2Suite extends StreamTest with BeforeAndAfter {
  test("data writer") {
    val partition = 1234
    val writer = new MemoryDataWriter(partition, OutputMode.Append())
    writer.write(Row(1))
    writer.write(Row(2))
    writer.write(Row(44))
    val msg = writer.commit()
    assert(msg.data.map(_.getInt(0)) == Seq(1, 2, 44))
    assert(msg.partition == partition)

    // Buffer should be cleared, so repeated commits should give empty.
    assert(writer.commit().data.isEmpty)
  }

  test("continuous writer") {
    val sink = new MemorySinkV2
    val writer = new MemoryStreamWriter(sink, OutputMode.Append(), DataSourceOptions.empty())
    writer.commit(0,
      Array(
        MemoryWriterCommitMessage(0, Seq(Row(1), Row(2))),
        MemoryWriterCommitMessage(1, Seq(Row(3), Row(4))),
        MemoryWriterCommitMessage(2, Seq(Row(6), Row(7)))
      ))
    assert(sink.latestBatchId.contains(0))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(1, 2, 3, 4, 6, 7))
    writer.commit(19,
      Array(
        MemoryWriterCommitMessage(3, Seq(Row(11), Row(22))),
        MemoryWriterCommitMessage(0, Seq(Row(33)))
      ))
    assert(sink.latestBatchId.contains(19))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(11, 22, 33))

    assert(sink.allData.map(_.getInt(0)).sorted == Seq(1, 2, 3, 4, 6, 7, 11, 22, 33))
  }

  test("microbatch writer") {
    val sink = new MemorySinkV2
    new MemoryWriter(sink, 0, OutputMode.Append(), DataSourceOptions.empty()).commit(
      Array(
        MemoryWriterCommitMessage(0, Seq(Row(1), Row(2))),
        MemoryWriterCommitMessage(1, Seq(Row(3), Row(4))),
        MemoryWriterCommitMessage(2, Seq(Row(6), Row(7)))
      ))
    assert(sink.latestBatchId.contains(0))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(1, 2, 3, 4, 6, 7))
    new MemoryWriter(sink, 19, OutputMode.Append(), DataSourceOptions.empty()).commit(
      Array(
        MemoryWriterCommitMessage(3, Seq(Row(11), Row(22))),
        MemoryWriterCommitMessage(0, Seq(Row(33)))
      ))
    assert(sink.latestBatchId.contains(19))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(11, 22, 33))

    assert(sink.allData.map(_.getInt(0)).sorted == Seq(1, 2, 3, 4, 6, 7, 11, 22, 33))
  }

  test("continuous writer with row limit") {
    val sink = new MemorySinkV2
    val optionsMap = new scala.collection.mutable.HashMap[String, String]
    optionsMap.put(MemorySinkBase.MAX_MEMORY_SINK_ROWS, 7.toString())
    val options = new DataSourceOptions(optionsMap.toMap.asJava)
    val appendWriter = new MemoryStreamWriter(sink, OutputMode.Append(), options)
    appendWriter.commit(0, Array(
        MemoryWriterCommitMessage(0, Seq(Row(1), Row(2))),
        MemoryWriterCommitMessage(1, Seq(Row(3), Row(4))),
        MemoryWriterCommitMessage(2, Seq(Row(6), Row(7)))))
    assert(sink.latestBatchId.contains(0))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(1, 2, 3, 4, 6, 7))
    appendWriter.commit(19, Array(
        MemoryWriterCommitMessage(3, Seq(Row(11), Row(22))),
        MemoryWriterCommitMessage(0, Seq(Row(33)))))
    assert(sink.latestBatchId.contains(19))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(11))

    assert(sink.allData.map(_.getInt(0)).sorted == Seq(1, 2, 3, 4, 6, 7, 11))

    val completeWriter = new MemoryStreamWriter(sink, OutputMode.Complete(), options)
    completeWriter.commit(20, Array(
        MemoryWriterCommitMessage(4, Seq(Row(11), Row(22))),
        MemoryWriterCommitMessage(5, Seq(Row(33)))))
    assert(sink.latestBatchId.contains(20))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(11, 22, 33))
    completeWriter.commit(21, Array(
      MemoryWriterCommitMessage(0, Seq(Row(1), Row(2), Row(3))),
      MemoryWriterCommitMessage(1, Seq(Row(4), Row(5), Row(6))),
      MemoryWriterCommitMessage(2, Seq(Row(7), Row(8), Row(9)))))
    assert(sink.latestBatchId.contains(21))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(1, 2, 3, 4, 5, 6, 7))

    assert(sink.allData.map(_.getInt(0)).sorted == Seq(1, 2, 3, 4, 5, 6, 7))
  }

  test("microbatch writer with row limit") {
    val sink = new MemorySinkV2
    val optionsMap = new scala.collection.mutable.HashMap[String, String]
    optionsMap.put(MemorySinkBase.MAX_MEMORY_SINK_ROWS, 5.toString())
    val options = new DataSourceOptions(optionsMap.toMap.asJava)

    new MemoryWriter(sink, 25, OutputMode.Append(), options).commit(Array(
      MemoryWriterCommitMessage(0, Seq(Row(1), Row(2))),
      MemoryWriterCommitMessage(1, Seq(Row(3), Row(4)))))
    assert(sink.latestBatchId.contains(25))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(1, 2, 3, 4))
    assert(sink.allData.map(_.getInt(0)).sorted == Seq(1, 2, 3, 4))
    new MemoryWriter(sink, 26, OutputMode.Append(), options).commit(Array(
      MemoryWriterCommitMessage(2, Seq(Row(5), Row(6))),
      MemoryWriterCommitMessage(3, Seq(Row(7), Row(8)))))
    assert(sink.latestBatchId.contains(26))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(5))
    assert(sink.allData.map(_.getInt(0)).sorted == Seq(1, 2, 3, 4, 5))

    new MemoryWriter(sink, 27, OutputMode.Complete(), options).commit(Array(
      MemoryWriterCommitMessage(4, Seq(Row(9), Row(10))),
      MemoryWriterCommitMessage(5, Seq(Row(11), Row(12)))))
    assert(sink.latestBatchId.contains(27))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(9, 10, 11, 12))
    assert(sink.allData.map(_.getInt(0)).sorted == Seq(9, 10, 11, 12))
    new MemoryWriter(sink, 28, OutputMode.Complete(), options).commit(Array(
      MemoryWriterCommitMessage(4, Seq(Row(13), Row(14), Row(15))),
      MemoryWriterCommitMessage(5, Seq(Row(16), Row(17), Row(18)))))
    assert(sink.latestBatchId.contains(28))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(13, 14, 15, 16, 17))
    assert(sink.allData.map(_.getInt(0)).sorted == Seq(13, 14, 15, 16, 17))
  }
}
