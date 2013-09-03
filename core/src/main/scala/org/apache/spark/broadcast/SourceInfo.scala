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

package org.apache.spark.broadcast

import java.util.BitSet

import org.apache.spark._

/**
 * Used to keep and pass around information of peers involved in a broadcast
 */
private[spark] case class SourceInfo (hostAddress: String,
                       listenPort: Int,
                       totalBlocks: Int = SourceInfo.UnusedParam,
                       totalBytes: Int = SourceInfo.UnusedParam)
extends Comparable[SourceInfo] with Logging {

  var currentLeechers = 0
  var receptionFailed = false

  var hasBlocks = 0
  var hasBlocksBitVector: BitSet = new BitSet (totalBlocks)

  // Ascending sort based on leecher count
  def compareTo (o: SourceInfo): Int = (currentLeechers - o.currentLeechers)
}

/**
 * Helper Object of SourceInfo for its constants
 */
private[spark] object SourceInfo {
  // Broadcast has not started yet! Should never happen.
  val TxNotStartedRetry = -1
  // Broadcast has already finished. Try default mechanism.
  val TxOverGoToDefault = -3
  // Other constants
  val StopBroadcast = -2
  val UnusedParam = 0
}
