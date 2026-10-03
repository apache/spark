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

package org.apache.spark.status

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.status.api.v1.AccumulableInfo
import org.apache.spark.status.protobuf.KVStoreProtobufSerializer
import org.apache.spark.util.kvstore.{CompactInMemoryStore, InMemoryStore, KVTypeInfo}

class CompactTaskDataCodecSuite extends SparkFunSuite {
  private val serializer = new KVStoreProtobufSerializer()
  private val typeInfo = new KVTypeInfo(classOf[TaskDataWrapper])
  private val indices = typeInfo.indices().iterator().asScala.map(_.value()).toSeq

  private def task(
      id: Int,
      status: String,
      stage: Int = 3,
      detailSize: Int = 0): TaskDataWrapper = {
    def metric(column: Int): Long = {
      if (id % 7 == 0 && column == 12) Long.MinValue
      else if (id % 5 == 0) 0L
      else if (status == "SUCCESS") id.toLong * column
      else -(id.toLong * column) - 1
    }
    new TaskDataWrapper(
      taskId = id.toLong,
      index = id,
      attempt = id,
      partitionId = id + 1,
      launchTime = 1000000000000L + id,
      resultFetchStart = -1L,
      duration = 1000L + id,
      speculative = (id & 1) != 0,
      hasMetrics = (id & 2) != 0,
      executorDeserializeTime = metric(9),
      executorDeserializeCpuTime = metric(10),
      executorRunTime = metric(11),
      executorCpuTime = metric(12),
      resultSize = metric(13),
      jvmGcTime = metric(14),
      resultSerializationTime = metric(15),
      memoryBytesSpilled = metric(16),
      diskBytesSpilled = metric(17),
      peakExecutionMemory = metric(18),
      inputBytesRead = metric(19),
      inputRecordsRead = metric(20),
      outputBytesWritten = metric(21),
      outputRecordsWritten = metric(22),
      shuffleRemoteBlocksFetched = metric(23),
      shuffleLocalBlocksFetched = metric(24),
      shuffleFetchWaitTime = metric(25),
      shuffleRemoteBytesRead = metric(26),
      shuffleRemoteBytesReadToDisk = metric(27),
      shuffleLocalBytesRead = metric(28),
      shuffleRecordsRead = metric(29),
      shuffleCorruptMergedBlockChunks = metric(30),
      shuffleMergedFetchFallbackCount = metric(31),
      shuffleMergedRemoteBlocksFetched = metric(32),
      shuffleMergedLocalBlocksFetched = metric(33),
      shuffleMergedRemoteChunksFetched = metric(34),
      shuffleMergedLocalChunksFetched = metric(35),
      shuffleMergedRemoteBytesRead = metric(36),
      shuffleMergedLocalBytesRead = metric(37),
      shuffleRemoteReqsDuration = metric(38),
      shuffleMergedRemoteReqDuration = metric(39),
      shuffleBytesWritten = metric(40),
      shuffleWriteTime = metric(41),
      shuffleRecordsWritten = metric(42),
      executorId = (id % 3).toString,
      host = s"host-${id % 3}",
      status = status,
      taskLocality = "PROCESS_LOCAL",
      accumulatorUpdates = if (detailSize > 0) {
        Seq(new AccumulableInfo(id, "counter", Some("update" * detailSize), "value" * detailSize))
      } else if (id % 2 == 0) Nil else {
        Seq(new AccumulableInfo(id, "counter", Some("update"), "value"))
      },
      errorMessage = if (status == "FAILED") {
        Some(s"error $id" + (" repeated failure" * detailSize))
      } else {
        None
      },
      stageId = stage,
      stageAttemptId = 1)
  }

  private def check(
      expected: TaskDataWrapper,
      record: org.apache.spark.util.kvstore.KVStoreRecordCodec.Record[TaskDataWrapper]): Unit = {
    assert(serializer.serialize(record.decode()).toSeq == serializer.serialize(expected).toSeq)
    indices.foreach { index =>
      val expectedIndex = typeInfo.getIndexValue(index, expected)
      val actualIndex = record.indexValue(index)
      expectedIndex match {
        case array: Array[Int] => assert(array.sameElements(actualIndex.asInstanceOf[Array[Int]]))
        case _ => assert(actualIndex == expectedIndex, s"Index $index differs")
      }
    }
  }

  test("active, pending and sealed task records preserve every field and index") {
    val codec = new CompactTaskDataCodec(blockSize = 17)
    val inputs = (0 until 75).map { id =>
      task(id, Seq("RUNNING", "GET RESULT", "SUCCESS", "FAILED", "KILLED")(id % 5))
    }
    val records = inputs.map(input => codec.encode(input))
    inputs.zip(records).foreach { case (input, record) => check(input, record) }
    codec.compact()
    inputs.zip(records).foreach { case (input, record) => check(input, record) }
  }

  test("removed snapshots stay readable after packing, stage eviction and late updates") {
    val codec = new CompactTaskDataCodec(blockSize = 4, maxOpenBlocks = 2)
    val first = task(1, "FAILED")
    val removed = codec.encode(first)
    removed.removed()
    val records = (2 until 30).map { id =>
      val input = task(id, "SUCCESS", id % 4)
      input -> codec.encode(input)
    }
    records.take(10).foreach(_._2.removed())
    codec.compact()
    check(first, removed)
    records.foreach { case (input, record) => check(input, record) }
    val late = task(1, "SUCCESS")
    check(late, codec.encode(late))
    codec.compact()
    check(first, removed)
  }

  test("completed task cold details round-trip before and after column compaction") {
    val codec = new CompactTaskDataCodec(blockSize = 32)
    val rows = (0 until 20).map { id =>
      val input = task(id, "FAILED", detailSize = 2048)
      input -> codec.encode(input)
    }
    rows.foreach { case (input, record) => check(input, record) }
    codec.compact()
    rows.foreach { case (input, record) => check(input, record) }
  }

  test("readers retain immutable task snapshots while batches are sealed and removed") {
    val codec = new CompactTaskDataCodec(blockSize = 512)
    val snapshots = (0 until 200).map { id =>
      val input = task(id, "SUCCESS")
      input -> codec.encode(input)
    }
    val executor = Executors.newSingleThreadExecutor()
    val started = new CountDownLatch(1)
    try {
      val reader = executor.submit(new Callable[Unit] {
        override def call(): Unit = {
          started.countDown()
          (0 until 4).foreach { _ =>
            snapshots.foreach { case (input, record) => check(input, record) }
          }
        }
      })
      assert(started.await(30, TimeUnit.SECONDS))
      snapshots.take(25).foreach(_._2.removed())
      codec.compact()
      snapshots.drop(25).foreach(_._2.removed())
      (0 until 200).foreach(id => codec.encode(task(id, "FAILED")))
      codec.compact()
      reader.get(30, TimeUnit.SECONDS)
    } finally {
      executor.shutdownNow()
      assert(executor.awaitTermination(30, TimeUnit.SECONDS))
    }
  }

  test("packed task pages, exact quantiles and eviction match the in-memory store") {
    val baseline = new InMemoryStore()
    val compact = new CompactInMemoryStore(serializer)
    compact.registerCodec(classOf[TaskDataWrapper], new CompactTaskDataCodec(blockSize = 17))
    try {
      (0 until 200).foreach { id =>
        val input = task(id, if (id % 5 == 0) "FAILED" else "SUCCESS")
        baseline.write(input)
        compact.write(input)
      }
      compact.compact()
      Seq(1, 6, 11, 16).foreach { id =>
        baseline.delete(classOf[TaskDataWrapper], id.toLong)
        compact.delete(classOf[TaskDataWrapper], id.toLong)
      }
      val expected = new AppStatusStore(baseline)
      val actual = new AppStatusStore(compact)
      indices.filterNot(_ == TaskIndexNames.STAGE).foreach { index =>
        Seq(true, false).foreach { ascending =>
          val sortBy = if (index == "__main__") None else Some(index)
          val expectedIds = expected.taskList(3, 1, 11, 13, sortBy, ascending).map(_.taskId)
          val actualIds = actual.taskList(3, 1, 11, 13, sortBy, ascending).map(_.taskId)
          assert(actualIds == expectedIds, s"Page differs for $index, ascending=$ascending")
        }
      }
      val quantiles = Array(0.0, 0.25, 0.5, 0.75, 1.0)
      val expectedSummary = expected.taskSummary(3, 1, quantiles).get
      val actualSummary = actual.taskSummary(3, 1, quantiles).get
      def checkQuantiles(actualValues: Seq[Double], expectedValues: Seq[Double]): Unit = {
        assert(actualValues.map(java.lang.Double.doubleToLongBits) ==
          expectedValues.map(java.lang.Double.doubleToLongBits))
      }
      checkQuantiles(actualSummary.executorRunTime, expectedSummary.executorRunTime)
      checkQuantiles(actualSummary.executorCpuTime, expectedSummary.executorCpuTime)
      checkQuantiles(actualSummary.shuffleReadMetrics.readBytes,
        expectedSummary.shuffleReadMetrics.readBytes)
      val (page, count) = actual.taskListWithFilter(3, 1, 7, 9,
        Some(TaskIndexNames.TASK_INDEX), ascending = false)(_.executorId == "1")
      val matches = expected.taskList(3, 1, 0, 200,
        Some(TaskIndexNames.TASK_INDEX), ascending = false).filter(_.executorId == "1")
      assert(count == matches.size)
      assert(page.map(_.taskId) == matches.slice(7, 16).map(_.taskId))
      compact.removeAllByIndexValues(classOf[TaskDataWrapper], TaskIndexNames.STAGE,
        Seq(Array(3, 1)).asJava)
      assert(compact.count(classOf[TaskDataWrapper]) == 0)
    } finally {
      baseline.close()
      compact.close()
    }
  }
}
