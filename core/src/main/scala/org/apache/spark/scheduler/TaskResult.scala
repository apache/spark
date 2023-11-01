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

package org.apache.spark.scheduler

import java.io._
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.internal.config.TASK_BLOCK_FETCH_BATCH_SIZE
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.serializer.{IteratorSerializerUtils, SerializerInstance}
import org.apache.spark.storage.BlockId
import org.apache.spark.util.{AccumulatorV2, Utils}

// Task result. Also contains updates to accumulator variables and executor metric peaks.
private[spark] sealed trait TaskResult[T] {
  def setAccumUpdates(accumUpdates: Seq[AccumulatorV2[_, _]]): Unit
  def getAccumUpdates(): Seq[AccumulatorV2[_, _]]

  def getMetricPeaks(): Array[Long]

  def value(resultSer: SerializerInstance = null): T

}

/** A reference to a DirectTaskResult that has been stored in the worker's BlockManager. */
private[spark] case class IndirectTaskResult[T](blockId: BlockId,
  size: Int, var accumUpdates: Seq[AccumulatorV2[_, _]],
  var metricPeaks: Array[Long],
  var isValueIterator: Boolean = false)
  extends TaskResult[T] with Serializable {


  def getAccumUpdates(): Seq[AccumulatorV2[_, _]] = accumUpdates

  def setAccumUpdates(accumUpdates: Seq[AccumulatorV2[_, _]]): Unit = {
     this.accumUpdates = accumUpdates
  }

  override def getMetricPeaks(): Array[Long] = metricPeaks


  override def value(resultSer: SerializerInstance): T = {
    IteratorSerializerUtils.deserialize(
      SparkEnv.get.blockManager.getRemoteBlockAsIterator(
        blockId, SparkEnv.get.conf.get(TASK_BLOCK_FETCH_BATCH_SIZE).intValue())
    ).asInstanceOf[T]
  }
}


/** A TaskResult that contains the task's return value, accumulator updates and metric peaks. */
private[spark] class DirectTaskResult[T](
  var valueBytes: ByteBuffer,
  var accumUpdates: Seq[AccumulatorV2[_, _]],
  var metricPeaks: Array[Long],
  var isValueIterator: Boolean = false)
  extends TaskResult[T] with Externalizable {

  private var valueObjectDeserialized = false
  private var valueObject: T = _

  def this() = this(null.asInstanceOf[ByteBuffer], null,
    new Array[Long](ExecutorMetricType.numMetrics))


  override def getAccumUpdates(): Seq[AccumulatorV2[_, _]] = accumUpdates

  override def getMetricPeaks(): Array[Long] = metricPeaks

  def setAccumUpdates(accumUpdates: Seq[AccumulatorV2[_, _]]): Unit = {
    this.accumUpdates = accumUpdates
  }
  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeInt(valueBytes.remaining)
    Utils.writeByteBuffer(valueBytes, out)
    out.writeInt(accumUpdates.size)
    accumUpdates.foreach(out.writeObject)
    out.writeInt(metricPeaks.length)
    metricPeaks.foreach(out.writeLong)
    out.writeBoolean(isValueIterator)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    val blen = in.readInt()
    val byteVal = new Array[Byte](blen)
    in.readFully(byteVal)
    valueBytes = ByteBuffer.wrap(byteVal)

    val numUpdates = in.readInt
    if (numUpdates == 0) {
      accumUpdates = Seq.empty
    } else {
      val _accumUpdates = new ArrayBuffer[AccumulatorV2[_, _]]
      for (i <- 0 until numUpdates) {
        _accumUpdates += in.readObject.asInstanceOf[AccumulatorV2[_, _]]
      }
      accumUpdates = _accumUpdates.toSeq
    }

    val numMetrics = in.readInt
    if (numMetrics == 0) {
      metricPeaks = Array.empty
    } else {
      metricPeaks = new Array[Long](numMetrics)
      (0 until numMetrics).foreach { i =>
        metricPeaks(i) = in.readLong
      }
    }
    isValueIterator = in.readBoolean()
    valueObjectDeserialized = false
  }


  /**
   * When `value()` is called at the first time, it needs to deserialize `valueObject` from
   * `valueBytes`. It may cost dozens of seconds for a large instance. So when calling `value` at
   * the first time, the caller should avoid to block other threads.
   *
   * After the first time, `value()` is trivial and just returns the deserialized `valueObject`.
   */
  def value(resultSer: SerializerInstance = null): T = {
    if (valueObjectDeserialized) {
      valueObject
    } else {
      // This should not run when holding a lock because it may cost dozens of seconds for a large
      // value
      val ser = if (resultSer == null) SparkEnv.get.serializer.newInstance() else resultSer
      if (!isValueIterator) {
        valueObject = ser.deserialize(valueBytes)
      } else {
        valueObject = IteratorSerializerUtils.deserialize(
          Array(valueBytes).toIterator).asInstanceOf[T]
      }
      valueObjectDeserialized = true
      valueObject
    }
  }
}
