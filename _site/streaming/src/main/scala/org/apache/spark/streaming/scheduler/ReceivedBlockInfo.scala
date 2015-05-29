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

package org.apache.spark.streaming.scheduler

import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.receiver.{ReceivedBlockStoreResult, WriteAheadLogBasedStoreResult}
import org.apache.spark.streaming.util.WriteAheadLogRecordHandle

/** Information about blocks received by the receiver */
private[streaming] case class ReceivedBlockInfo(
    streamId: Int,
    numRecords: Long,
    metadataOption: Option[Any],
    blockStoreResult: ReceivedBlockStoreResult
  ) {

  @volatile private var _isBlockIdValid = true

  def blockId: StreamBlockId = blockStoreResult.blockId

  def walRecordHandleOption: Option[WriteAheadLogRecordHandle] = {
    blockStoreResult match {
      case walStoreResult: WriteAheadLogBasedStoreResult => Some(walStoreResult.walRecordHandle)
      case _ => None
    }
  }

  /** Is the block ID valid, that is, is the block present in the Spark executors. */
  def isBlockIdValid(): Boolean = _isBlockIdValid

  /**
   * Set the block ID as invalid. This is useful when it is known that the block is not present
   * in the Spark executors.
   */
  def setBlockIdInvalid(): Unit = {
    _isBlockIdValid = false
  }
}

