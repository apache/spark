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

package org.apache.spark.executor

import org.apache.spark.{Accumulator, InternalAccumulator}
import org.apache.spark.annotation.DeveloperApi


/**
 * :: DeveloperApi ::
 * Method by which input data was read. Network means that the data was read over the network
 * from a remote block manager (which may have stored the data on-disk or in-memory).
 * Operations are not thread-safe.
 */
@DeveloperApi
object DataReadMethod extends Enumeration with Serializable {
  type DataReadMethod = Value
  val Memory, Disk, Hadoop, Network = Value
}


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represents metrics about reading data from external systems.
 */
@DeveloperApi
class InputMetrics private (
    _bytesRead: Accumulator[Long],
    _recordsRead: Accumulator[Long],
    _readMethod: Accumulator[String])
  extends Serializable {

  private[executor] def this(accumMap: Map[String, Accumulator[_]]) {
    this(
      TaskMetrics.getAccum[Long](accumMap, InternalAccumulator.input.BYTES_READ),
      TaskMetrics.getAccum[Long](accumMap, InternalAccumulator.input.RECORDS_READ),
      TaskMetrics.getAccum[String](accumMap, InternalAccumulator.input.READ_METHOD))
  }

  /**
   * Create a new [[InputMetrics]] that is not associated with any particular task.
   *
   * This mainly exists because of SPARK-5225, where we are forced to use a dummy [[InputMetrics]]
   * because we want to ignore metrics from a second read method. In the future, we should revisit
   * whether this is needed.
   *
   * A better alternative is [[TaskMetrics.registerInputMetrics]].
   */
  private[executor] def this() {
    this(InternalAccumulator.createInputAccums()
      .map { a => (a.name.get, a) }.toMap[String, Accumulator[_]])
  }

  /**
   * Total number of bytes read.
   */
  def bytesRead: Long = _bytesRead.localValue

  /**
   * Total number of records read.
   */
  def recordsRead: Long = _recordsRead.localValue

  /**
   * The source from which this task reads its input.
   */
  def readMethod: DataReadMethod.Value = DataReadMethod.withName(_readMethod.localValue)

  // Once incBytesRead & intRecordsRead is ready to be removed from the public API
  // we can remove the internal versions and make the previous public API private.
  // This has been done to suppress warnings when building.
  @deprecated("incrementing input metrics is for internal use only", "2.0.0")
  def incBytesRead(v: Long): Unit = _bytesRead.add(v)
  private[spark] def incBytesReadInternal(v: Long): Unit = _bytesRead.add(v)
  @deprecated("incrementing input metrics is for internal use only", "2.0.0")
  def incRecordsRead(v: Long): Unit = _recordsRead.add(v)
  private[spark] def incRecordsReadInternal(v: Long): Unit = _recordsRead.add(v)
  private[spark] def setBytesRead(v: Long): Unit = _bytesRead.setValue(v)
  private[spark] def setReadMethod(v: DataReadMethod.Value): Unit =
    _readMethod.setValue(v.toString)

}

/**
 * Deprecated methods to preserve case class matching behavior before Spark 2.0.
 */
object InputMetrics {

  @deprecated("matching on InputMetrics will not be supported in the future", "2.0.0")
  def apply(readMethod: DataReadMethod.Value): InputMetrics = {
    val im = new InputMetrics
    im.setReadMethod(readMethod)
    im
  }

  @deprecated("matching on InputMetrics will not be supported in the future", "2.0.0")
  def unapply(input: InputMetrics): Option[DataReadMethod.Value] = {
    Some(input.readMethod)
  }
}
