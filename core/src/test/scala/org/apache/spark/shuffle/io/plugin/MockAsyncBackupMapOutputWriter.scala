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

import java.io.{ByteArrayOutputStream, FilterOutputStream, OutputStream}
import java.nio.file.{Files, Path}

import scala.collection.mutable
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

import org.apache.spark.shuffle.api.{ShuffleMapOutputWriter, ShufflePartitionWriter}
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage

class MockAsyncBackupMapOutputWriter(
    shuffleBackupManager: MockAsyncShuffleBlockBackupManager,
    localDelegate: ShuffleMapOutputWriter,
    shuffleId: Int,
    mapId: Long,
    private implicit val backupExecutionContext: ExecutionContext)
  extends ShuffleMapOutputWriter {

  private var backupTasks: mutable.Set[Future[Path]] = mutable.Set[Future[Path]]()
  private val backupIdsByPartition: mutable.Map[Int, String] = mutable.Map[Int, String]()

  override def getPartitionWriter(reducePartitionId: Int): ShufflePartitionWriter = {
    localDelegate.getPartitionWriter(reducePartitionId)
  }

  override def commitAllPartitions(): MapOutputCommitMessage = {
    val delegateMessage = localDelegate.commitAllPartitions()
    val metadata = MockAsyncBackupMapOutputMetadata(
      backupIdsByPartition.toMap,
      delegateMessage.getMapOutputMetadata.asScala)
    MapOutputCommitMessage.of(delegateMessage.getPartitionLengths, metadata)
  }

  override def abort(error: Throwable): Unit = {
    localDelegate.abort(error)
    backupTasks.foreach { task =>
      task.onComplete {
        case Success(path) => Files.delete(path)
      }
    }
    shuffleBackupManager.deleteShufflesWithBackupIds(shuffleId, backupIdsByPartition.values.toSet)
  }

  private class MockAsyncBackupShufflePartitionWriter(reducePartitionId: Int)
      extends ShufflePartitionWriter {
    private val localDelegatePartWriter = localDelegate.getPartitionWriter(reducePartitionId)
    private val partBytesInMemoryOutput = new ByteArrayOutputStream()

    override def openStream(): OutputStream = {
      new FilterOutputStream(localDelegatePartWriter.openStream()) {

        override def write(b: Int): Unit = {
          super.write(b)
          partBytesInMemoryOutput.write(b)
        }

        override def write(b: Array[Byte], off: Int, len: Int): Unit = {
          super.write(b, off, len)
          partBytesInMemoryOutput.write(b, off, len)
        }

        override def close(): Unit = {
          super.close()
          val (backupId, task) = shuffleBackupManager.backupBlock(
            shuffleId, mapId, reducePartitionId, partBytesInMemoryOutput.toByteArray)
          backupTasks += task
          backupIdsByPartition(reducePartitionId) = backupId
        }
      }
    }

    override def getNumBytesWritten: Long = localDelegatePartWriter.getNumBytesWritten
  }
}
