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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.LongAccumulator


/**
 * :: DeveloperApi ::
 * Method by which input data was read. Network means that the data was read over the network
 * from a remote block manager (which may have stored the data on-disk or in-memory).
 * Operations are not thread-safe.
 */
@DeveloperApi
object DataReadMethod extends Enumeration with Serializable {
  type DataReadMethod = Value
  val MEMORY, DISK, HADOOP, NETWORK = Value

  @deprecated("Use MEMORY instead.", "3.0.0")
  val Memory = MEMORY
  @deprecated("Use DISK instead.", "3.0.0")
  val Disk = DISK
  @deprecated("Use HADOOP instead.", "3.0.0")
  val Hadoop = HADOOP
  @deprecated("Use NETWORK instead.", "3.0.0")
  val Network = NETWORK
}


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represents metrics about reading data from external systems.
 */
@DeveloperApi
class InputMetrics private[spark] () extends Serializable {
  private[executor] val _bytesRead = new LongAccumulator
  private[executor] val _recordsRead = new LongAccumulator

  /**
   * Total number of bytes read.
   */
  def bytesRead: Long = _bytesRead.sum

  /**
   * Total number of records read.
   */
  def recordsRead: Long = _recordsRead.sum

  private[spark] def incBytesRead(v: Long): Unit = _bytesRead.add(v)
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)
  private[spark] def setBytesRead(v: Long): Unit = _bytesRead.setValue(v)
}
