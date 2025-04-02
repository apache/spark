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
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.serializer.{SerializerHelper, SerializerInstance}
import org.apache.spark.storage.BlockId
import org.apache.spark.util.{AccumulatorV2, Utils}
import org.apache.spark.util.io.ChunkedByteBuffer

// Task result. Also contains updates to accumulator variables and executor metric peaks.
private[spark] sealed trait TaskResult[T]

/** A reference to a DirectTaskResult that has been stored in the worker's BlockManager. */
private[spark] case class IndirectTaskResult[T](blockId: BlockId, size: Long)
  extends TaskResult[T] with Serializable

/** A TaskResult that contains the task's return value, accumulator updates and metric peaks. */
private[spark] class DirectTaskResult[T](
    var valueByteBuffer: ChunkedByteBuffer,
    var accumUpdates: Seq[AccumulatorV2[_, _]],
    var metricPeaks: Array[Long])
  extends TaskResult[T] with Externalizable {

  private var valueObjectDeserialized = false
  private var valueObject: T = _

  def this(
    valueByteBuffer: ByteBuffer,
    accumUpdates: Seq[AccumulatorV2[_, _]],
    metricPeaks: Array[Long]) = {
    this(new ChunkedByteBuffer(Array(valueByteBuffer)), accumUpdates, metricPeaks)
  }

  def this() = this(null.asInstanceOf[ChunkedByteBuffer], Seq(),
    new Array[Long](ExecutorMetricType.numMetrics))

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    valueByteBuffer.writeExternal(out)
    out.writeInt(accumUpdates.size)
    accumUpdates.foreach(out.writeObject)
    out.writeInt(metricPeaks.length)
    metricPeaks.foreach(out.writeLong)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    valueByteBuffer = new ChunkedByteBuffer()
    valueByteBuffer.readExternal(in)

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
      valueObject = SerializerHelper.deserializeFromChunkedBuffer(ser, valueByteBuffer)
      valueObjectDeserialized = true
      valueObject
    }
  }
}
