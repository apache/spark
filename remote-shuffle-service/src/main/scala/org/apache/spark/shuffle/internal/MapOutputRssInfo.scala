/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.internal


import org.apache.spark.remoteshuffle.util.StringUtils

import scala.collection.JavaConverters._

/**
 * This class stores RSS information which is retrieved from map output tracker
 *
 * @param numMaps
 * @param numRssServers
 * @param taskAttemptIds
 */
case class MapOutputRssInfo(numMaps: Int, numRssServers: Int, taskAttemptIds: Array[Long]) {
  override def toString: String = {
    val taskAttemptIdsStr = StringUtils.toString4SortedNumberList[java.lang.Long](
      taskAttemptIds.sorted.map(long2Long).toList.asJava)
    s"MapOutputRssInfo(numMaps: $numMaps, numRssServers: $numRssServers, " +
      s"taskAttemptIds: $taskAttemptIdsStr)"
  }
}
