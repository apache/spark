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

package org.apache.spark.shuffle.io.plugin

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.shuffle.api.metadata.{MapOutputMetadata, ShuffleOutputTracker}
import org.apache.spark.storage.ShuffleBlockId

class MockAsyncBackupShuffleOutputTracker(
    backupManager: MockAsyncShuffleBlockBackupManager,
    localDelegateTracker: Option[ShuffleOutputTracker]) extends ShuffleOutputTracker {

  private val backupIdsByBlockId = new ConcurrentHashMap[ShuffleBlockId, String].asScala

  override def registerShuffle(shuffleId: Int): Unit = {
    localDelegateTracker.foreach(_.registerShuffle(shuffleId))
  }

  override def unregisterShuffle(shuffleId: Int, blocking: Boolean): Unit = {
    localDelegateTracker.foreach(_.unregisterShuffle(shuffleId, blocking))
    backupManager.deleteAllShufflesWithShuffleId(shuffleId)
  }

  override def registerMapOutput(
      shuffleId: Int,
      mapId: Int,
      mapTaskAttemptId: Long,
      mapOutputMetadata: MapOutputMetadata): Unit = {
    val mockMapOutputMetadata = cast(mapOutputMetadata)
    localDelegateTracker.foreach { tracker =>
      mockMapOutputMetadata.delegateMetadata.foreach { metadata =>
        tracker.registerMapOutput(shuffleId, mapId, mapTaskAttemptId, metadata)
      }
    }
    mockMapOutputMetadata
      .backupIdsByPartition
      .foreach { case (partitionId, backupId) =>
        backupIdsByBlockId(ShuffleBlockId(shuffleId, mapTaskAttemptId, partitionId)) = backupId
      }
  }

  override def removeMapOutput(shuffleId: Int, mapId: Int, mapTaskAttemptId: Long): Unit = {
    localDelegateTracker.foreach(_.removeMapOutput(shuffleId, mapId, mapTaskAttemptId))
    backupManager.deleteShufflesWithBackupIds(
      shuffleId,
      backupIdsByBlockId
        .filterKeys(key => key.shuffleId == shuffleId && key.mapId == mapTaskAttemptId)
        .values
        .toSet)
  }

  private def cast(generic: MapOutputMetadata): MockAsyncBackupMapOutputMetadata = {
    require(generic.isInstanceOf[MockAsyncBackupMapOutputMetadata],
      s"Received unsupported type of output metadata: ${generic.getClass}")
    generic.asInstanceOf[MockAsyncBackupMapOutputMetadata]
  }
}
