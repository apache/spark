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

package org.apache.spark.sql.streaming.continuous.shuffle

import scala.collection.mutable

import org.apache.spark.{HashPartitioner, Partition, TaskContext, TaskContextImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.continuous.shuffle.{ContinuousShuffleReadPartition, ContinuousShuffleReadRDD, RPCContinuousShuffleWriter}
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{DataType, IntegerType}

class ContinuousShuffleSuite extends StreamTest {
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

  private case class SimplePartition(index: Int) extends Partition

  /**
   * An RDD that simulates multiple continuous processing epochs, with each epoch corresponding
   * to one entry in the outer epochData array. The data in the inner array is round-robined across
   * the specified number of partitions.
   */
  private class MultipleEpochRDD(numPartitions: Int, epochData: Array[Int]*)
      extends RDD[UnsafeRow](sparkContext, Nil) {
    override def getPartitions: Array[Partition] = {
      (0 until numPartitions).map(SimplePartition).toArray
    }

    private val currentEpochForPartition = mutable.Map[Int, Int]().withDefaultValue(0)

    override def compute(split: Partition, ctx: TaskContext): Iterator[UnsafeRow] = {
      val epoch = epochData(currentEpochForPartition(split.index)).zipWithIndex.collect {
        case (value, idx) if idx % numPartitions == split.index => unsafeRow(value)
      }

      currentEpochForPartition(split.index) += 1
      epoch.toIterator
    }
  }

  private implicit def unsafeRow(value: Int) = {
    UnsafeProjection.create(Array(IntegerType : DataType))(
      new GenericInternalRow(Array(value: Any)))
  }

  private def readRDDEndpoint(rdd: ContinuousShuffleReadRDD) = {
    rdd.partitions(0).asInstanceOf[ContinuousShuffleReadPartition].endpoint
  }

  private def readEpoch(rdd: ContinuousShuffleReadRDD) = {
    rdd.compute(rdd.partitions(0), ctx).toSeq.map(_.getInt(0))
  }

  test("one epoch") {
    val reader = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1)
    val writer = new RPCContinuousShuffleWriter(
      0, new HashPartitioner(1), Array(readRDDEndpoint(reader)))

    writer.write(Iterator(1, 2, 3))

    assert(readEpoch(reader) == Seq(1, 2, 3))
  }

  test("multiple epochs") {
    val reader = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1)
    val writer = new RPCContinuousShuffleWriter(
      0, new HashPartitioner(1), Array(readRDDEndpoint(reader)))

    writer.write(Iterator(1, 2, 3))
    writer.write(Iterator(4, 5, 6))

    assert(readEpoch(reader) == Seq(1, 2, 3))
    assert(readEpoch(reader) == Seq(4, 5, 6))
  }

  test("empty epochs") {
    val data = new MultipleEpochRDD(1, Array(), Array(1, 2), Array(), Array(), Array(3, 4), Array())

    val reader = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1)
    val writer = new RPCContinuousShuffleWriter(
      0, new HashPartitioner(1), Array(readRDDEndpoint(reader)))

    writer.write(Iterator())
    writer.write(Iterator(1, 2))
    writer.write(Iterator())
    writer.write(Iterator())
    writer.write(Iterator(3, 4))
    writer.write(Iterator())

    assert(readEpoch(reader) == Seq())
    assert(readEpoch(reader) == Seq(1, 2))
    assert(readEpoch(reader) == Seq())
    assert(readEpoch(reader) == Seq())
    assert(readEpoch(reader) == Seq(3, 4))
    assert(readEpoch(reader) == Seq())
  }

  test("blocks waiting for writer") {
    val reader = new ContinuousShuffleReadRDD(sparkContext, numPartitions = 1)
    val writer = new RPCContinuousShuffleWriter(
      0, new HashPartitioner(1), Array(readRDDEndpoint(reader)))

    val readerEpoch = reader.compute(reader.partitions(0), ctx)

    val readRowThread = new Thread {
      override def run(): Unit = {
        assert(readerEpoch.toSeq.map(_.getInt(0)) == Seq(1))
      }
    }
    readRowThread.start()

    eventually(timeout(streamingTimeout)) {
      assert(readRowThread.getState == Thread.State.TIMED_WAITING)
    }

    // Once we write the epoch the thread should stop waiting and succeed.
    writer.write(Iterator(1))
    readRowThread.join()
  }

  test("multiple writer partitions") {
    val numWriterPartitions = 3

    val reader = new ContinuousShuffleReadRDD(
      sparkContext, numPartitions = 1, numShuffleWriters = numWriterPartitions)
    val writers = (0 until 3).map { idx =>
      new RPCContinuousShuffleWriter(idx, new HashPartitioner(1), Array(readRDDEndpoint(reader)))
    }

    writers(0).write(Iterator(1, 4, 7))
    writers(1).write(Iterator(2, 5))
    writers(2).write(Iterator(3, 6))

    writers(0).write(Iterator(4, 7, 10))
    writers(1).write(Iterator(5, 8))
    writers(2).write(Iterator(6, 9))

    // Since there are multiple asynchronous writers, the original row sequencing is not guaranteed.
    // The epochs should be deterministically preserved, however.
    assert(readEpoch(reader).toSet == Seq(1, 2, 3, 4, 5, 6, 7).toSet)
    assert(readEpoch(reader).toSet == Seq(4, 5, 6, 7, 8, 9, 10).toSet)
  }

  test("reader epoch only ends when all writer partitions write it") {
    val numWriterPartitions = 3
    val data = new MultipleEpochRDD(numWriterPartitions, Array())

    val reader = new ContinuousShuffleReadRDD(
      sparkContext, numPartitions = 1, numShuffleWriters = numWriterPartitions)
    val writers = (0 until 3).map { idx =>
      new RPCContinuousShuffleWriter(idx, new HashPartitioner(1), Array(readRDDEndpoint(reader)))
    }

    writers(1).write(Iterator())
    writers(2).write(Iterator())

    val readerEpoch = reader.compute(reader.partitions(0), ctx)

    val readEpochMarkerThread = new Thread {
      override def run(): Unit = {
        assert(!readerEpoch.hasNext)
      }
    }

    readEpochMarkerThread.start()
    eventually(timeout(streamingTimeout)) {
      assert(readEpochMarkerThread.getState == Thread.State.TIMED_WAITING)
    }

    writers(0).write(Iterator())
    readEpochMarkerThread.join()
  }
}
