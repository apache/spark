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
 * Method by which output data was written.
 * Operations are not thread-safe.
 */
@DeveloperApi
object DataWriteMethod extends Enumeration with Serializable {
  type DataWriteMethod = Value
  val Hadoop = Value
}


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represents metrics about writing data to external systems.
 */
@DeveloperApi
class OutputMetrics private (
    _bytesWritten: Accumulator[Long],
    _recordsWritten: Accumulator[Long],
    _writeMethod: Accumulator[String])
  extends Serializable {

  private[executor] def this(accumMap: Map[String, Accumulator[_]]) {
    this(
      TaskMetrics.getAccum[Long](accumMap, InternalAccumulator.output.BYTES_WRITTEN),
      TaskMetrics.getAccum[Long](accumMap, InternalAccumulator.output.RECORDS_WRITTEN),
      TaskMetrics.getAccum[String](accumMap, InternalAccumulator.output.WRITE_METHOD))
  }

  /**
   * Create a new [[OutputMetrics]] that is not associated with any particular task.
   *
   * This is only used for preserving matching behavior on [[OutputMetrics]], which used to be
   * a case class before Spark 2.0. Once we remove support for matching on [[OutputMetrics]]
   * we can remove this constructor as well.
   */
  private[executor] def this() {
    this(InternalAccumulator.createOutputAccums()
      .map { a => (a.name.get, a) }.toMap[String, Accumulator[_]])
  }

  /**
   * Total number of bytes written.
   */
  def bytesWritten: Long = _bytesWritten.localValue

  /**
   * Total number of records written.
   */
  def recordsWritten: Long = _recordsWritten.localValue

  /**
   * The source to which this task writes its output.
   */
  def writeMethod: DataWriteMethod.Value = DataWriteMethod.withName(_writeMethod.localValue)

  private[spark] def setBytesWritten(v: Long): Unit = _bytesWritten.setValue(v)
  private[spark] def setRecordsWritten(v: Long): Unit = _recordsWritten.setValue(v)
  private[spark] def setWriteMethod(v: DataWriteMethod.Value): Unit =
    _writeMethod.setValue(v.toString)

}

/**
 * Deprecated methods to preserve case class matching behavior before Spark 2.0.
 */
object OutputMetrics {

  @deprecated("matching on OutputMetrics will not be supported in the future", "2.0.0")
  def apply(writeMethod: DataWriteMethod.Value): OutputMetrics = {
    val om = new OutputMetrics
    om.setWriteMethod(writeMethod)
    om
  }

  @deprecated("matching on OutputMetrics will not be supported in the future", "2.0.0")
  def unapply(output: OutputMetrics): Option[DataWriteMethod.Value] = {
    Some(output.writeMethod)
  }
}
