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

package org.apache.spark.sql.execution.streaming.continuous

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset

class EpochMarkerAwareIterator(
    context: TaskContext,
    startOffset: PartitionOffset,
    child: EpochMarkerAwareIterator) extends Iterator[UnsafeRow] {
  private val POLL_TIMEOUT_MS = 1000

  private var currentEntry: (UnsafeRow, PartitionOffset) = _
  private var currentOffset: PartitionOffset = startOffset
  private var currentEpoch =
    context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong

  override def hasNext(): Boolean = {
    child.hasNext()
  }

  override def next(): UnsafeRow = {
    val row = child.next()
    if (row == null) {
      handleMarker()
      row
    } else {

    }
  }

  def handleMarker(): Unit = {}

  def handleRow(row: UnsafeRow): = {}
}
