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


/**
 * :: DeveloperApi ::
 * Method by which input data was read.  Network means that the data was read over the network
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
case class InputMetrics(readMethod: DataReadMethod.Value) {

  /**
   * This is volatile so that it is visible to the updater thread.
   */
  @volatile @transient var bytesReadCallback: Option[() => Long] = None

  /**
   * Total bytes read.
   */
  private var _bytesRead: Long = _
  def bytesRead: Long = _bytesRead
  def incBytesRead(bytes: Long): Unit = _bytesRead += bytes

  /**
   * Total records read.
   */
  private var _recordsRead: Long = _
  def recordsRead: Long = _recordsRead
  def incRecordsRead(records: Long): Unit = _recordsRead += records

  /**
   * Invoke the bytesReadCallback and mutate bytesRead.
   */
  def updateBytesRead() {
    bytesReadCallback.foreach { c =>
      _bytesRead = c()
    }
  }

  /**
   * Register a function that can be called to get up-to-date information on how many bytes the task
   * has read from an input source.
   */
  def setBytesReadCallback(f: Option[() => Long]) {
    bytesReadCallback = f
  }
}
