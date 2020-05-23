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

import java.io.File
import java.nio.file.Files
import java.util

import scala.concurrent.ExecutionContext

import org.apache.spark.shuffle.api.{ShuffleExecutorComponents, ShuffleMapOutputWriter}
import org.apache.spark.util.ThreadUtils

class MockAsyncBackupShuffleExecutorComponents(
      localDelegate: ShuffleExecutorComponents) extends ShuffleExecutorComponents {

  private var backupExecutionContext: ExecutionContext = _
  private var backupManager: MockAsyncShuffleBlockBackupManager = _

  override def initializeExecutor(
      appId: String, execId: String, extraConfigs: util.Map[String, String]): Unit = {
    localDelegate.initializeExecutor(appId, execId, extraConfigs)
    val backupDirPath = extraConfigs.get(MockAsyncBackupShuffleDataIO.BACKUP_DIR)
    require(backupDirPath != null,
      s"Backup path must be specified with ${MockAsyncBackupShuffleDataIO.BACKUP_DIR}")
    val backupDir = new File(backupDirPath)
    Files.createDirectories(backupDir.toPath)
    val backupExecutor = ThreadUtils.newDaemonSingleThreadExecutor("test-shuffle-backups")
    backupExecutionContext = ExecutionContext.fromExecutorService(backupExecutor)
    backupManager = new MockAsyncShuffleBlockBackupManager(backupDir, backupExecutionContext)
  }

  override def createMapOutputWriter(shuffleId: Int, mapTaskId: Long, numPartitions: Int)
      : ShuffleMapOutputWriter = {
    val delegateWriter = localDelegate.createMapOutputWriter(shuffleId, mapTaskId, numPartitions)
    new MockAsyncBackupMapOutputWriter(
      backupManager,
      delegateWriter,
      shuffleId,
      mapTaskId,
      backupExecutionContext)
  }
}
