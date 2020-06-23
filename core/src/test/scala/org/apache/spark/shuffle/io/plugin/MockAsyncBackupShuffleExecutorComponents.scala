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
      localDelegate: ShuffleExecutorComponents,
      backupDirPath: String) extends ShuffleExecutorComponents {

  private val backupDir = new File(backupDirPath)
  private val backupExecutor = ThreadUtils.newDaemonSingleThreadExecutor("test-shuffle-backups")
  private val backupManager = new MockAsyncShuffleBlockBackupManager(
    backupDir, backupExecutionContext)
  private val backupExecutionContext = ExecutionContext.fromExecutorService(backupExecutor)

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
