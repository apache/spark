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
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ExecutorService}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils

class MockAsyncShuffleBlockBackupManager(
    backupDir: File,
    private implicit val backupExecutionContext: ExecutionContext) {

  private val backupTasksLock = new Object
  @GuardedBy("backupTasksLock")
  private val backupTasks: mutable.Map[ShuffleBlockId, Future[Path]] =
    new ConcurrentHashMap[ShuffleBlockId, Future[Path]].asScala

  def backupBlock(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      block: Array[Byte]): (String, Future[Path]) = {
    val backupId = UUID.randomUUID.toString
    val blockId = ShuffleBlockId(shuffleId, mapId, partitionId)
    backupTasksLock.synchronized {
      val task = Future[Path] {
        val partitionFile = shuffleDir(shuffleId).resolve(backupId)
        Files.write(partitionFile, block, StandardOpenOption.CREATE_NEW)
        partitionFile
      }
      backupTasks(ShuffleBlockId(shuffleId, mapId, partitionId)) = task
      task.onComplete { _ => backupTasks.remove(blockId) }
      (backupId, task)
    }
  }

  def deleteAllShufflesWithShuffleId(shuffleId: Int): Unit = {
    backupTasksLock.synchronized {
      backupTasks.filterKeys(blockId => blockId.shuffleId == shuffleId)
        .values
        .foreach { task =>
          task.onComplete {
            case Success(file) => Files.delete(file)
          }
        }
      }
      Utils.deleteRecursively(backupDir.toPath.resolve(shuffleId.toString).toFile)
  }

  def deleteShufflesWithBackupIds(shuffleId: Int, backupIds: Set[String]): Unit = {
    backupIds.foreach { backupId =>
      Files.delete(shuffleDir(shuffleId).resolve(backupId))
    }
  }

  def getBlock(shuffleId: Int, backupId: String): Array[Byte] = {
    Files.readAllBytes(shuffleDir(shuffleId).resolve(backupId))
  }

  private def shuffleDir(shuffleId: Int): Path = {
    backupDir.toPath.resolve(shuffleId.toString)
  }
}
