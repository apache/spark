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
  val Memory, Disk, Hadoop, Network = Value
}


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represents metrics about reading data from external systems.
 */
@DeveloperApi
class InputMetrics private[spark] () extends Serializable {
  private[executor] val _bytesRead = new LongAccumulator
  private[executor] val _recordsRead = new LongAccumulator
  private[executor] val _skipBloomBlocks = new LongAccumulator
  private[executor] val _skipBloomRows = new LongAccumulator
  private[executor] val _totalBloomBlocks = new LongAccumulator
  private[executor] val _footerReadTime = new LongAccumulator
  private[executor] val _footerReadNumber = new LongAccumulator
  private[executor] val _totalPagesCount = new LongAccumulator
  private[executor] val _filteredPagesCount = new LongAccumulator
  private[executor] val _afterFilterPagesCount = new LongAccumulator

  /**
   * Total number of bytes read.
   */
  def bytesRead: Long = _bytesRead.sum

  /**
   * Total number of records read.
   */
  def recordsRead: Long = _recordsRead.sum

  def totalBloomBlocks: Long = _totalBloomBlocks.sum

  def totalSkipBloomBlocks: Long = _skipBloomBlocks.sum

  def totalSkipBloomRows: Long = _skipBloomRows.sum

  def footerReadTime: Long = _footerReadTime.sum

  def footerReadNumber: Long = _footerReadNumber.sum
  def totalPagesCount: Long = _totalPagesCount.sum
  def filteredPagesCount: Long = _filteredPagesCount.sum
  def afterFilterPagesCount: Long = _afterFilterPagesCount.sum

  private[spark] def incBytesRead(v: Long): Unit = _bytesRead.add(v)
  private[spark] def incSkipBloomBlocks(v: Long): Unit = _skipBloomBlocks.add(v)
  private[spark] def incSkipRows(v: Long): Unit = _skipBloomRows.add(v)
  private[spark] def incTotalBloomBlocks(v: Long): Unit = _totalBloomBlocks.add(v)
  private[spark] def incFooterReadTime(v: Long): Unit = _footerReadTime.add(v)
  private[spark] def incFooterReadNumber(v: Long): Unit = _footerReadNumber.add(v)
  private[spark] def incTotalPagesCount(v: Long): Unit = _totalPagesCount.add(v)
  private[spark] def incFilteredPagesCount(v: Long): Unit = _filteredPagesCount.add(v)
  private[spark] def incAfterFilterPagesCount(v: Long): Unit = _afterFilterPagesCount.add(v)
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)
  private[spark] def setBytesRead(v: Long): Unit = _bytesRead.setValue(v)
}
