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

import org.apache.spark.{Accumulable, Accumulator, InternalAccumulator}
import org.apache.spark.annotation.DeveloperApi


/**
 * :: DeveloperApi ::
 * Method by which input data was read. Network means that the data was read over the network
 * from a remote block manager (which may have stored the data on-disk or in-memory).
 */
@DeveloperApi
object DataReadMethod extends Enumeration with Serializable {
  type DataReadMethod = Value
  val Memory, Disk, Hadoop, Network = Value
}


/**
 * :: DeveloperApi ::
 * Metrics about reading input data.
 */
@DeveloperApi
class InputMetrics private (
    val readMethod: DataReadMethod.Value,
    _bytesRead: Accumulator[Long],
    _recordsRead: Accumulator[Long])
  extends Serializable {

  private[executor] def this(
      readMethod: DataReadMethod.Value,
      accumMap: Map[String, Accumulable[_, _]]) {
    this(
      readMethod,
      TaskMetrics.getLongAccum(accumMap, InternalAccumulator.input.BYTES_READ),
      TaskMetrics.getLongAccum(accumMap, InternalAccumulator.input.RECORDS_READ))
  }

  /**
   * Create a new [[InputMetrics]] that is not associated with any particular task.
   *
   * This mainly exists because of SPARK-5225, where we are forced to use a dummy [[InputMetrics]]
   * because we want to ignore metrics from a second read method. In the future, we should revisit
   * whether this is needed.
   *
   * A better alternative to use is [[TaskMetrics.registerInputMetrics]].
   */
  private[spark] def this(readMethod: DataReadMethod.Value) {
    this(
      readMethod,
      InternalAccumulator.createInputAccums().map { a => (a.name.get, a) }.toMap)
  }

  /**
   * Total number of bytes read.
   */
  def bytesRead: Long = _bytesRead.localValue

  /**
   * Total number of records read.
   */
  def recordsRead: Long = _recordsRead.localValue

  private[spark] def setBytesRead(v: Long): Unit = _bytesRead.add(v)
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)
}
