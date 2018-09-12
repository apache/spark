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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.sources._
import org.apache.spark.sql.streaming.{OutputMode, StreamTest}
import org.apache.spark.sql.types.StructType

class MemorySinkV2Suite extends StreamTest with BeforeAndAfter {
  test("data writer") {
    val partition = 1234
    val writer = new MemoryDataWriter(
      partition, OutputMode.Append(), new StructType().add("i", "int"))
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
    val sink = new MemorySinkV2
    val writeSupport = new MemoryStreamWriter(
      sink, OutputMode.Append(), new StructType().add("i", "int"))
    writeSupport.commit(0,
      Array(
        MemoryWriterCommitMessage(0, Seq(Row(1), Row(2))),
        MemoryWriterCommitMessage(1, Seq(Row(3), Row(4))),
        MemoryWriterCommitMessage(2, Seq(Row(6), Row(7)))
      ))
    assert(sink.latestBatchId.contains(0))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(1, 2, 3, 4, 6, 7))
    writeSupport.commit(19,
      Array(
        MemoryWriterCommitMessage(3, Seq(Row(11), Row(22))),
        MemoryWriterCommitMessage(0, Seq(Row(33)))
      ))
    assert(sink.latestBatchId.contains(19))
    assert(sink.latestBatchData.map(_.getInt(0)).sorted == Seq(11, 22, 33))

    assert(sink.allData.map(_.getInt(0)).sorted == Seq(1, 2, 3, 4, 6, 7, 11, 22, 33))
  }
}
