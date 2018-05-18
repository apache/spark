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
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{DataType, IntegerType}

class ContinuousShuffleReadSuite extends StreamTest {

  private def unsafeRow(value: Int) = {
    UnsafeProjection.create(Array(IntegerType : DataType))(
      new GenericInternalRow(Array(value: Any)))
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
    endpoint.askSync[Unit](ReceiverEpochMarker())
    endpoint.askSync[Unit](ReceiverRow(unsafeRow(111)))

    ctx.markTaskCompleted(None)
    val receiver = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].receiver
    eventually(timeout(streamingTimeout)) {
      assert(receiver.stopped.get())
    }
  }

  test("receiver stopped with marker last") {
    val rdd = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1)
    val endpoint = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].endpoint
    endpoint.askSync[Unit](ReceiverRow(unsafeRow(111)))
    endpoint.askSync[Unit](ReceiverEpochMarker())

    ctx.markTaskCompleted(None)
    val receiver = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].receiver
    eventually(timeout(streamingTimeout)) {
      assert(receiver.stopped.get())
    }
  }

  test("one epoch") {
    val rdd = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1)
    val endpoint = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].endpoint
    endpoint.askSync[Unit](ReceiverRow(unsafeRow(111)))
    endpoint.askSync[Unit](ReceiverRow(unsafeRow(222)))
    endpoint.askSync[Unit](ReceiverRow(unsafeRow(333)))
    endpoint.askSync[Unit](ReceiverEpochMarker())

    val iter = rdd.compute(rdd.partitions(0), ctx)
    assert(iter.toSeq.map(_.getInt(0)) == Seq(111, 222, 333))
  }

  test("multiple epochs") {
    val rdd = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1)
    val endpoint = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].endpoint
    endpoint.askSync[Unit](ReceiverRow(unsafeRow(111)))
    endpoint.askSync[Unit](ReceiverEpochMarker())
    endpoint.askSync[Unit](ReceiverRow(unsafeRow(222)))
    endpoint.askSync[Unit](ReceiverRow(unsafeRow(333)))
    endpoint.askSync[Unit](ReceiverEpochMarker())

    val firstEpoch = rdd.compute(rdd.partitions(0), ctx)
    assert(firstEpoch.toSeq.map(_.getInt(0)) == Seq(111))

    val secondEpoch = rdd.compute(rdd.partitions(0), ctx)
    assert(secondEpoch.toSeq.map(_.getInt(0)) == Seq(222, 333))
  }

  test("empty epochs") {
    val rdd = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1)
    val endpoint = rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].endpoint
    endpoint.askSync[Unit](ReceiverEpochMarker())
    endpoint.askSync[Unit](ReceiverEpochMarker())
    endpoint.askSync[Unit](ReceiverRow(unsafeRow(111)))
    endpoint.askSync[Unit](ReceiverEpochMarker())
    endpoint.askSync[Unit](ReceiverEpochMarker())
    endpoint.askSync[Unit](ReceiverEpochMarker())

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
      part.endpoint.askSync[Unit](ReceiverRow(unsafeRow(part.index)))
      part.endpoint.askSync[Unit](ReceiverEpochMarker())
    }

    for (p <- rdd.partitions) {
      val part = p.asInstanceOf[ContinuousShuffleReadPartition]
      val iter = rdd.compute(part, ctx)
      assert(iter.next().getInt(0) == part.index)
      assert(!iter.hasNext)
    }
  }

  test("blocks waiting for new rows") {
    val rdd = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1)

    val readRow = new Thread {
      override def run(): Unit = {
        // set the non-inheritable thread local
        TaskContext.setTaskContext(ctx)
        val epoch = rdd.compute(rdd.partitions(0), ctx)
        epoch.next().getInt(0)
      }
    }

    readRow.start()
    eventually(timeout(streamingTimeout)) {
      assert(readRow.getState == Thread.State.WAITING)
    }
  }
}
