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

import scala.collection.mutable.ArrayBuffer

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.storage.BlockId
import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator, Utils}

// Task result. Also contains updates to accumulator variables.
private[spark] sealed trait TaskResult[T]

/** A reference to a DirectTaskResult that has been stored in the worker's BlockManager. */
private[spark] case class IndirectTaskResult[T](blockId: BlockId, size: Int)
  extends TaskResult[T] with Serializable

/** A TaskResult that contains the task's return value and accumulator updates. */
private[spark] class DirectTaskResult[T](
    private var _value: Any,
    var accumUpdates: Seq[AccumulatorV2[_, _]],
    private val serializationTimeMetric: Option[DoubleAccumulator] = None)
  extends TaskResult[T] with Externalizable with KryoSerializable {

  def this() = this(null, null)

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    serializationTimeMetric match {
      case Some(timeMetric) =>
        val start = System.nanoTime()
        out.writeObject(_value)
        out.writeInt(accumUpdates.size + 1)
        accumUpdates.foreach(out.writeObject)
        val end = System.nanoTime()
        timeMetric.setValue(math.max(end - start, 0L) / 1000000.0)
        out.writeObject(timeMetric)
      case None =>
        out.writeObject(_value)
        out.writeInt(accumUpdates.size)
        accumUpdates.foreach(out.writeObject)
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    _value = in.readObject()

    val numUpdates = in.readInt
    if (numUpdates == 0) {
      accumUpdates = null
    } else {
      val _accumUpdates = new ArrayBuffer[AccumulatorV2[_, _]]
      for (i <- 0 until numUpdates) {
        _accumUpdates += in.readObject.asInstanceOf[AccumulatorV2[_, _]]
      }
      accumUpdates = _accumUpdates
    }
  }

  override def write(kryo: Kryo, output: Output): Unit = Utils.tryOrIOException {
    serializationTimeMetric match {
      case Some(timeMetric) =>
        val start = System.nanoTime()
        kryo.writeClassAndObject(output, _value)
        output.writeVarInt(accumUpdates.size, true)
        output.writeBoolean(true) // indicates additional timeMetric
        accumUpdates.foreach(kryo.writeClassAndObject(output, _))
        val end = System.nanoTime()
        timeMetric.setValue(math.max(end - start, 0L) / 1000000.0)
        timeMetric.write(kryo, output)
      case None =>
        kryo.writeClassAndObject(output, _value)
        output.writeVarInt(accumUpdates.size, true)
        output.writeBoolean(false) // indicates no timeMetric
        accumUpdates.foreach(kryo.writeClassAndObject(output, _))
    }
  }

  override def read(kryo: Kryo, input: Input): Unit = Utils.tryOrIOException {
    _value = kryo.readClassAndObject(input)

    var numUpdates = input.readVarInt(true)
    val hasTimeMetric = input.readBoolean()
    if (numUpdates == 0 && !hasTimeMetric) {
      accumUpdates = null
    } else {
      val _accumUpdates = new ArrayBuffer[AccumulatorV2[_, _]](
        if (hasTimeMetric) numUpdates + 1 else numUpdates)
      while (numUpdates > 0) {
        _accumUpdates += kryo.readClassAndObject(input)
            .asInstanceOf[AccumulatorV2[_, _]]
        numUpdates -= 1
      }
      if (hasTimeMetric) {
        val timeMetric = new DoubleAccumulator
        timeMetric.read(kryo, input)
        _accumUpdates += timeMetric
      }
      accumUpdates = _accumUpdates
    }
  }

  def value(): T = _value.asInstanceOf[T]
}
