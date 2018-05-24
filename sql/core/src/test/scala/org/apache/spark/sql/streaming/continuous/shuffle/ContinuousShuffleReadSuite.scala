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

package org.apache.spark.sql.execution.streaming.continuous.shuffle

import org.apache.spark.{TaskContext, TaskContextImpl}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class ContinuousShuffleReadSuite extends StreamTest {

  private def unsafeRow(value: Int) = {
    UnsafeProjection.create(Array(IntegerType : DataType))(
      new GenericInternalRow(Array(value: Any)))
  }

  private def unsafeRow(value: String) = {
    UnsafeProjection.create(Array(StringType : DataType))(
      new GenericInternalRow(Array(UTF8String.fromString(value): Any)))
  }

  private def send(endpoint: RpcEndpointRef, messages: UnsafeRowReceiverMessage*) = {
    messages.foreach(endpoint.askSync[Unit](_))
  }

  // In this unit test, we emulate that we're in the task thread where
  // ContinuousShuffleReadRDD.compute() will be evaluated. This requires a task context
  // thread local to be set.
  var ctx: TaskContextImpl = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    ctx = TaskContext.empty()
    TaskContext.setTaskContext(ctx)
  }

  override def afterEach(): Unit = {
    ctx.markTaskCompleted(None)
    TaskContext.unset()
    ctx = null
    super.afterEach()
  }

  test("receiver stopped with row last") {
    val rdd = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1)
    val endpoint = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].endpoint
    send(
      endpoint,
      ReceiverEpochMarker(0),
      ReceiverRow(0, unsafeRow(111))
    )

    ctx.markTaskCompleted(None)
    val receiver = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].reader
    eventually(timeout(streamingTimeout)) {
      assert(receiver.asInstanceOf[UnsafeRowReceiver].stopped.get())
    }
  }

  test("receiver stopped with marker last") {
    val rdd = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1)
    val endpoint = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].endpoint
    send(
      endpoint,
      ReceiverRow(0, unsafeRow(111)),
      ReceiverEpochMarker(0)
    )

    ctx.markTaskCompleted(None)
    val receiver = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].reader
    eventually(timeout(streamingTimeout)) {
      assert(receiver.asInstanceOf[UnsafeRowReceiver].stopped.get())
    }
  }

  test("one epoch") {
    val rdd = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1)
    val endpoint = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].endpoint
    send(
      endpoint,
      ReceiverRow(0, unsafeRow(111)),
      ReceiverRow(0, unsafeRow(222)),
      ReceiverRow(0, unsafeRow(333)),
      ReceiverEpochMarker(0)
    )

    val iter = rdd.compute(rdd.partitions(0), ctx)
    assert(iter.toSeq.map(_.getInt(0)) == Seq(111, 222, 333))
  }

  test("multiple epochs") {
    val rdd = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1)
    val endpoint = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].endpoint
    send(
      endpoint,
      ReceiverRow(0, unsafeRow(111)),
      ReceiverEpochMarker(0),
      ReceiverRow(0, unsafeRow(222)),
      ReceiverRow(0, unsafeRow(333)),
      ReceiverEpochMarker(0)
    )

    val firstEpoch = rdd.compute(rdd.partitions(0), ctx)
    assert(firstEpoch.toSeq.map(_.getInt(0)) == Seq(111))

    val secondEpoch = rdd.compute(rdd.partitions(0), ctx)
    assert(secondEpoch.toSeq.map(_.getInt(0)) == Seq(222, 333))
  }

  test("empty epochs") {
    val rdd = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1)
    val endpoint = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].endpoint

    send(
      endpoint,
      ReceiverEpochMarker(0),
      ReceiverEpochMarker(0),
      ReceiverRow(0, unsafeRow(111)),
      ReceiverEpochMarker(0),
      ReceiverEpochMarker(0),
      ReceiverEpochMarker(0)
    )

    assert(rdd.compute(rdd.partitions(0), ctx).isEmpty)
    assert(rdd.compute(rdd.partitions(0), ctx).isEmpty)

    val thirdEpoch = rdd.compute(rdd.partitions(0), ctx)
    assert(thirdEpoch.toSeq.map(_.getInt(0)) == Seq(111))

    assert(rdd.compute(rdd.partitions(0), ctx).isEmpty)
    assert(rdd.compute(rdd.partitions(0), ctx).isEmpty)
  }

  test("multiple partitions") {
    val rdd = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 5)
    // Send all data before processing to ensure there's no crossover.
    for (p <- rdd.partitions) {
      val part = p.asInstanceOf[ContinuousShuffleReadPartition]
      // Send index for identification.
      send(
        part.endpoint,
        ReceiverRow(0, unsafeRow(part.index)),
        ReceiverEpochMarker(0)
      )
    }

    for (p <- rdd.partitions) {
      val part = p.asInstanceOf[ContinuousShuffleReadPartition]
      val iter = rdd.compute(part, ctx)
      assert(iter.next().getInt(0) == part.index)
      assert(!iter.hasNext)
    }
  }

  test("blocks waiting for new rows") {
    val rdd = new ContinuousShuffleReadRDD(
      sparkContext, numPartitions = 1, epochIntervalMs = Long.MaxValue)
    val epoch = rdd.compute(rdd.partitions(0), ctx)

    val readRowThread = new Thread {
      override def run(): Unit = {
        try {
          epoch.next().getInt(0)
        } catch {
          case _: InterruptedException => // do nothing - expected at test ending
        }
      }
    }

    try {
      readRowThread.start()
      eventually(timeout(streamingTimeout)) {
        assert(readRowThread.getState == Thread.State.TIMED_WAITING)
      }
    } finally {
      readRowThread.interrupt()
      readRowThread.join()
    }
  }

  test("multiple writers") {
    val rdd = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1, numShuffleWriters = 3)
    val endpoint = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].endpoint
    send(
      endpoint,
      ReceiverRow(0, unsafeRow("writer0-row0")),
      ReceiverRow(1, unsafeRow("writer1-row0")),
      ReceiverRow(2, unsafeRow("writer2-row0")),
      ReceiverEpochMarker(0),
      ReceiverEpochMarker(1),
      ReceiverEpochMarker(2)
    )

    val firstEpoch = rdd.compute(rdd.partitions(0), ctx)
    assert(firstEpoch.toSeq.map(_.getUTF8String(0).toString).toSet ==
      Set("writer0-row0", "writer1-row0", "writer2-row0"))
  }

  test("epoch only ends when all writers send markers") {
    val rdd = new ContinuousShuffleReadRDD(
      sparkContext, numPartitions = 1, numShuffleWriters = 3, epochIntervalMs = Long.MaxValue)
    val endpoint = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].endpoint
    send(
      endpoint,
      ReceiverRow(0, unsafeRow("writer0-row0")),
      ReceiverRow(1, unsafeRow("writer1-row0")),
      ReceiverRow(2, unsafeRow("writer2-row0")),
      ReceiverEpochMarker(0),
      ReceiverEpochMarker(2)
    )

    val epoch = rdd.compute(rdd.partitions(0), ctx)
    val rows = (0 until 3).map(_ => epoch.next()).toSet
    assert(rows.map(_.getUTF8String(0).toString) ==
      Set("writer0-row0", "writer1-row0", "writer2-row0"))

    // After checking the right rows, block until we get an epoch marker indicating there's no next.
    // (Also fail the assertion if for some reason we get a row.)
    val readEpochMarkerThread = new Thread {
      override def run(): Unit = {
        assert(!epoch.hasNext)
      }
    }

    readEpochMarkerThread.start()
    eventually(timeout(streamingTimeout)) {
      assert(readEpochMarkerThread.getState == Thread.State.TIMED_WAITING)
    }

    // Send the last epoch marker - now the epoch should finish.
    send(endpoint, ReceiverEpochMarker(1))
    eventually(timeout(streamingTimeout)) {
      !readEpochMarkerThread.isAlive
    }

    // Join to pick up assertion failures.
    readEpochMarkerThread.join()
  }

  test("writer epochs non aligned") {
    val rdd = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1, numShuffleWriters = 3)
    val endpoint = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].endpoint
    // We send multiple epochs for 0, then multiple for 1, then multiple for 2. The receiver should
    // collate them as though the markers were aligned in the first place.
    send(
      endpoint,
      ReceiverRow(0, unsafeRow("writer0-row0")),
      ReceiverEpochMarker(0),
      ReceiverRow(0, unsafeRow("writer0-row1")),
      ReceiverEpochMarker(0),
      ReceiverEpochMarker(0),

      ReceiverEpochMarker(1),
      ReceiverRow(1, unsafeRow("writer1-row0")),
      ReceiverEpochMarker(1),
      ReceiverRow(1, unsafeRow("writer1-row1")),
      ReceiverEpochMarker(1),

      ReceiverEpochMarker(2),
      ReceiverEpochMarker(2),
      ReceiverRow(2, unsafeRow("writer2-row0")),
      ReceiverEpochMarker(2)
    )

    val firstEpoch = rdd.compute(rdd.partitions(0), ctx).map(_.getUTF8String(0).toString).toSet
    assert(firstEpoch == Set("writer0-row0"))

    val secondEpoch = rdd.compute(rdd.partitions(0), ctx).map(_.getUTF8String(0).toString).toSet
    assert(secondEpoch == Set("writer0-row1", "writer1-row0"))

    val thirdEpoch = rdd.compute(rdd.partitions(0), ctx).map(_.getUTF8String(0).toString).toSet
    assert(thirdEpoch == Set("writer1-row1", "writer2-row0"))
  }
}
