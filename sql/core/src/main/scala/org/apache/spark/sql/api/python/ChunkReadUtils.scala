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

package org.apache.spark.sql.api.python

import java.io.ByteArrayOutputStream

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.arrow.ArrowBatchStreamWriter
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.storage.{ArrowBatchBlockId, BlockId, StorageLevel}


object ChunkReadUtils {

  def persistDataFrameAsArrowBatchChunks(dataFrame: DataFrame): Array[String] = {
    val sparkSession = SparkSession.getActiveSession.get
    val rdd = dataFrame.toArrowBatchRdd
    val schemaJson = dataFrame.schema.json
    val timeZoneId = sparkSession.sessionState.conf.sessionLocalTimeZone
    val errorOnDuplicatedFieldNames =
      sparkSession.sessionState.conf.pandasStructHandlingMode == "legacy"
    rdd.mapPartitions { iter: Iterator[Array[Byte]] =>
      val blockManager = SparkEnv.get.blockManager
      val schema = DataType.fromJson(schemaJson).asInstanceOf[StructType]
      val chunkIds = new ArrayBuffer[String]()

      try {
        for (arrowBatch <- iter) {
          val uuid = java.util.UUID.randomUUID()
          val blockId = ArrowBatchBlockId(uuid)

          val out = new ByteArrayOutputStream(32 * 1024 * 1024)

          val batchWriter =
            new ArrowBatchStreamWriter(schema, out, timeZoneId, errorOnDuplicatedFieldNames)

          batchWriter.writeBatches(Iterator.single(arrowBatch))
          batchWriter.end()

          val blockData = out.toByteArray

          blockManager.putSingle[Array[Byte]](
            blockId, blockData, StorageLevel.MEMORY_AND_DISK, tellMaster = true
          )
          chunkIds.append(blockId.toString)
        }
      } catch {
        case e: Exception =>
          // Clean cached chunks
          unpersistChunks(chunkIds.asJava)
          throw e
      }

      chunkIds.iterator
    }.collect()
  }

  def unpersistChunks(chunkIds: java.util.List[String]): Unit = {
    val blockManager = SparkEnv.get.blockManager
    for (chunkId <- chunkIds.asScala) {
      try {
        blockManager.removeBlock(BlockId(chunkId))
      } catch {
        case _: Exception => ()
      }
    }
  }
}
