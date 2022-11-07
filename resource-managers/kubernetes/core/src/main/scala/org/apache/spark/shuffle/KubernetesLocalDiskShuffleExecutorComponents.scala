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

package org.apache.spark.shuffle

import java.io.File
import java.util.Optional

import scala.reflect.ClassTag

import org.apache.commons.io.FileExistsException

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.api.{ShuffleExecutorComponents, ShuffleMapOutputWriter, SingleSpillShuffleMapOutputWriter}
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleExecutorComponents
import org.apache.spark.storage.{BlockId, BlockManager, StorageLevel, UnrecognizedBlockId}
import org.apache.spark.util.Utils

class KubernetesLocalDiskShuffleExecutorComponents(sparkConf: SparkConf)
  extends ShuffleExecutorComponents with Logging {

  private val delegate = new LocalDiskShuffleExecutorComponents(sparkConf)
  private var blockManager: BlockManager = _

  override def initializeExecutor(
      appId: String, execId: String, extraConfigs: java.util.Map[String, String]): Unit = {
    delegate.initializeExecutor(appId, execId, extraConfigs)
    blockManager = SparkEnv.get.blockManager
    if (sparkConf.getBoolean("spark.kubernetes.driver.reusePersistentVolumeClaim", false)) {
      // Turn off the deletion of the shuffle data in order to reuse
      blockManager.diskBlockManager.deleteFilesOnStop = false
      Utils.tryLogNonFatalError {
        KubernetesLocalDiskShuffleExecutorComponents.recoverDiskStore(sparkConf, blockManager)
      }
    }
  }

  override def createMapOutputWriter(shuffleId: Int, mapTaskId: Long, numPartitions: Int)
    : ShuffleMapOutputWriter = {
    delegate.createMapOutputWriter(shuffleId, mapTaskId, numPartitions)
  }

  override def createSingleFileMapOutputWriter(shuffleId: Int, mapId: Long)
    : Optional[SingleSpillShuffleMapOutputWriter] = {
    delegate.createSingleFileMapOutputWriter(shuffleId, mapId)
  }
}

object KubernetesLocalDiskShuffleExecutorComponents extends Logging {
  /**
   * This tries to recover shuffle data of dead executors' local dirs if exists.
   * Since the executors are already dead, we cannot use `getHostLocalDirs`.
   * This is enabled only when spark.kubernetes.driver.reusePersistentVolumeClaim is true.
   */
  def recoverDiskStore(conf: SparkConf, bm: BlockManager): Unit = {
    // Find All files
    val files = Utils.getConfiguredLocalDirs(conf)
      .filter(_ != null)
      .map(s => new File(new File(new File(s).getParent).getParent))
      .flatMap { dir =>
        val oldDirs = dir.listFiles().filter { f =>
          f.isDirectory && f.getName.startsWith("spark-")
        }
        val files = oldDirs
          .flatMap(_.listFiles).filter(_.isDirectory) // executor-xxx
          .flatMap(_.listFiles).filter(_.isDirectory) // blockmgr-xxx
          .flatMap(_.listFiles).filter(_.isDirectory) // 00
          .flatMap(_.listFiles)
        if (files != null) files.toSeq else Seq.empty
      }

    logInfo(s"Found ${files.size} files")

    // This is not used.
    val classTag = implicitly[ClassTag[Object]]
    val level = StorageLevel.DISK_ONLY
    val (indexFiles, dataFiles) = files.partition(_.getName.endsWith(".index"))
    (dataFiles ++ indexFiles).foreach { f =>
      try {
        val id = BlockId(f.getName)
        val decryptedSize = f.length()
        bm.TempFileBasedBlockStoreUpdater(id, level, classTag, f, decryptedSize).save()
      } catch {
        case _: UnrecognizedBlockId =>
        case _: FileExistsException =>
          // This may happen due to recompute, but we continue to recover next files
      }
    }
  }
}

