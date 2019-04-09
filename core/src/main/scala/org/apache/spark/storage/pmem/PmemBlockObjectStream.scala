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

package org.apache.spark.storage.pmem

import java.io._

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import org.apache.spark.storage._
import org.apache.spark.storage.pmem.{PmemInputStream, PmemOutputStream}
import org.apache.spark.util.Utils

class PmemBlockId (stageId: Int, tmpId: Int) extends ShuffleBlockId(stageId, 0, tmpId) {
  override def name: String = "reduce_spill_" + stageId + "_" + tmpId
  override def isShuffle: Boolean = false
}

object PmemBlockId {
  private var tempId: Int = 0
  def getTempBlockId(stageId: Int): PmemBlockId = synchronized {
    val cur_tempId = tempId
    tempId += 1
    new PmemBlockId (stageId, cur_tempId)
  }
}

private[spark] class PmemBlockObjectStream(
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    taskMetrics: TaskMetrics,
    blockId: BlockId,
    conf: SparkConf,
    numMaps: Int = 0,
    numPartitions: Int = 0
) extends DiskBlockObjectWriter(
  new File(Utils.getConfiguredLocalDirs(conf).toList(0) + "/null"),
  null, null, 0, true, null, null) with Logging {
  var initialized = false

  var size: Int = 0
  var records: Int = 0

  var recordsPerBlock: Int = 0
  val recordsArray: ArrayBuffer[Int] = ArrayBuffer()
  var spilled: Boolean = false
  var partitionMeta: Array[(Long, Int, Int)] = _

  val root_dir = Utils.getConfiguredLocalDirs(conf).toList(0)
  val path_list = conf.get("spark.shuffle.pmem.pmem_list").split(",").map(_.trim).toList
  val maxPoolSize: Long = conf.getLong("spark.shuffle.pmem.pmpool_size",
                                       defaultValue = 1073741824)
  val maxStages: Int = conf.getInt("spark.shuffle.pmem.max_stage_num", defaultValue = 1000)
  val persistentMemoryWriter: PersistentMemoryHandler =
    PersistentMemoryHandler.getPersistentMemoryHandler(root_dir, path_list, blockId.name,
                                                       maxPoolSize, maxStages, numMaps)
  val spill_throttle = 4194304
  persistentMemoryWriter.updateShuffleMeta(blockId.name)
  logDebug(blockId.name)

  var objStream: SerializationStream = _
  var wrappedStream: OutputStream = _
  val bytesStream: OutputStream = new PmemOutputStream(
    persistentMemoryWriter, numPartitions, blockId.name, numMaps)
  var inputStream: InputStream = _

  override def write(key: Any, value: Any): Unit = {
    if (!initialized) {
      wrappedStream = serializerManager.wrapStream(blockId, bytesStream)
      objStream = serializerInstance.serializeStream(wrappedStream)
      initialized = true
    }
    objStream.writeObject(key)
    objStream.writeObject(value)
    records += 1
    recordsPerBlock += 1
    maybeSpill()
  }

  override def close() {
    if (initialized) {
      logDebug("PersistentMemoryHandlerPartition: stream closed.")
      objStream.close()
      bytesStream.close()
    }
  }

  def maybeSpill(force: Boolean = false): Unit = {
    val bufSize = bytesStream.asInstanceOf[PmemOutputStream].size
    if ((spill_throttle != -1 && bufSize >= spill_throttle) || force == true) {
      val start = System.nanoTime()
      objStream.flush()
      bytesStream.flush()

      recordsArray += recordsPerBlock
      recordsPerBlock = 0
      size += bufSize

      if (blockId.isShuffle == true) {
        val writeMetrics = taskMetrics.shuffleWriteMetrics
        writeMetrics.incWriteTime(System.nanoTime() - start)
        writeMetrics.incBytesWritten(bufSize)
      } else {
        taskMetrics.incDiskBytesSpilled(bufSize)
      }
      bytesStream.asInstanceOf[PmemOutputStream].reset()
      spilled = true
    }
  }

  def ifSpilled(): Boolean = {
    spilled
  }

  def getPartitionMeta(): Array[(Long, Int, Int)] = {
    if (partitionMeta == null) {
      var i = -1
      partitionMeta = persistentMemoryWriter.getPartitionBlockInfo(blockId.name)
        .map{ x => i += 1; (x._1, x._2, recordsArray(i))}
    }
    partitionMeta
  }

  def getBlockId(): BlockId = {
    blockId
  }

  def getRkey(): Long = {
    persistentMemoryWriter.rkey
  }

  def getTotalRecords(): Long = {
    records
  }

  def getSize(): Long = {
    size
  }

  def getInputStream(): InputStream = {
    if (inputStream == null) {
      inputStream = new PmemInputStream(persistentMemoryWriter, blockId.name)
    }
    inputStream
  }
}
