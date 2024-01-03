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

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, SparkEnv, TaskContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.{ArrowBatchStreamWriter, ArrowConverters}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.{ArrowBatchBlockId, BlockId, StorageLevel}


case class ChunkMeta(
  id: String,
  rowCount: Long,
  byteCount: Long
)

/**
 * A partition evaluator class to:
 *  1. convert spark DataFrame partition rows into arrow batches
 *  2. persist arrow batches to block manager using storage level "MEMORY_AND_DISK",
 *    each arrow batch is persisted as a "Array[Byte]" type block.
 */
class PersistDataFrameAsArrowBatchChunksPartitionEvaluator(
    schema: StructType,
    timeZoneId: String,
    errorOnDuplicatedFieldNames: Boolean,
    maxRecordsPerBatch: Long
) extends PartitionEvaluator[InternalRow, ChunkMeta] {

  def eval(partitionIndex: Int, inputs: Iterator[InternalRow]*): Iterator[ChunkMeta] = {
    val blockManager = SparkEnv.get.blockManager
    val chunkMetaList = new ArrayBuffer[ChunkMeta]()

    val context = TaskContext.get()
    val arrowBatchIter = ArrowConverters.toBatchIterator(
      inputs(0), schema, maxRecordsPerBatch, timeZoneId,
      errorOnDuplicatedFieldNames, context
    )

    try {
      while (arrowBatchIter.hasNext) {
        val arrowBatch = arrowBatchIter.next()
        val rowCount = arrowBatchIter.getRowCountInLastBatch

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
        chunkMetaList.append(
          ChunkMeta(blockId.toString, rowCount, blockData.length)
        )
      }
    } catch {
      case e: Exception =>
        // Clean cached chunks
        for (chunkMeta <- chunkMetaList) {
          try {
            blockManager.master.removeBlock(BlockId(chunkMeta.id))
          } catch {
            case _: Exception => ()
          }
        }
        throw e
    }

    chunkMetaList.iterator
  }
}

/**
 * A partition evaluator factory class to create
 * instance of `PersistDataFrameAsArrowBatchChunksPartitionEvaluator`.
 */
class PersistDataFrameAsArrowBatchChunksPartitionEvaluatorFactory(
    schema: StructType,
    timeZoneId: String,
    errorOnDuplicatedFieldNames: Boolean,
    maxRecordsPerBatch: Long
) extends PartitionEvaluatorFactory[InternalRow, ChunkMeta] {

  def createEvaluator(): PartitionEvaluator[InternalRow, ChunkMeta] = {
    new PersistDataFrameAsArrowBatchChunksPartitionEvaluator(
      schema, timeZoneId, errorOnDuplicatedFieldNames, maxRecordsPerBatch
    )
  }
}

object ChunkReadUtils {

  def persistDataFrameAsArrowBatchChunks(
      dataFrame: DataFrame, maxRecordsPerBatch: Int
  ): Array[ChunkMeta] = {
    val sparkSession = SparkSession.getActiveSession.get

    val maxRecordsPerBatchVal = if (maxRecordsPerBatch == -1) {
      sparkSession.sessionState.conf.arrowMaxRecordsPerBatch
    } else {
      maxRecordsPerBatch
    }
    val timeZoneId = sparkSession.sessionState.conf.sessionLocalTimeZone
    val errorOnDuplicatedFieldNames =
      sparkSession.sessionState.conf.pandasStructHandlingMode == "legacy"

    dataFrame.queryExecution.toRdd.mapPartitionsWithEvaluator(
      new PersistDataFrameAsArrowBatchChunksPartitionEvaluatorFactory(
        schema = dataFrame.schema,
        timeZoneId = timeZoneId,
        errorOnDuplicatedFieldNames = errorOnDuplicatedFieldNames,
        maxRecordsPerBatch = maxRecordsPerBatchVal
      )
    ).collect()
  }

  def unpersistChunks(chunkIds: java.util.List[String]): Unit = {
    val blockManagerMaster = SparkEnv.get.blockManager.master

    for (chunkId <- chunkIds.asScala) {
      try {
        blockManagerMaster.removeBlock(BlockId(chunkId))
      } catch {
        case _: Exception => ()
      }
    }
  }
}
