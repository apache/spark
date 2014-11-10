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

import scala.collection.mutable.Map

import org.apache.spark.SparkEnv
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.BlockId
import org.apache.spark.util.Utils

// Task result. Also contains updates to accumulator variables.
private[spark] sealed trait TaskResult[T]

/** A reference to a DirectTaskResult that has been stored in the worker's BlockManager. */
private[spark] case class IndirectTaskResult[T](blockId: BlockId, size: Int)
  extends TaskResult[T] with Serializable

/** A TaskResult that contains the task's return value and accumulator updates. */
private[spark]
class DirectTaskResult[T](var valueBytes: ByteBuffer, var accumUpdates: Map[Long, Any],
    var metrics: TaskMetrics)
  extends TaskResult[T] with Externalizable {

  def this() = this(null.asInstanceOf[ByteBuffer], null, null)

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {

    out.writeInt(valueBytes.remaining);
    Utils.writeByteBuffer(valueBytes, out)

    out.writeInt(accumUpdates.size)
    for ((key, value) <- accumUpdates) {
      out.writeLong(key)
      out.writeObject(value)
    }
    out.writeObject(metrics)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {

    val blen = in.readInt()
    val byteVal = new Array[Byte](blen)
    in.readFully(byteVal)
    valueBytes = ByteBuffer.wrap(byteVal)

    val numUpdates = in.readInt
    if (numUpdates == 0) {
      accumUpdates = null
    } else {
      accumUpdates = Map()
      for (i <- 0 until numUpdates) {
        accumUpdates(in.readLong()) = in.readObject()
      }
    }
    metrics = in.readObject().asInstanceOf[TaskMetrics]
  }

  def value(): T = {
    val resultSer = SparkEnv.get.serializer.newInstance()
    resultSer.deserialize(valueBytes)
  }
}
