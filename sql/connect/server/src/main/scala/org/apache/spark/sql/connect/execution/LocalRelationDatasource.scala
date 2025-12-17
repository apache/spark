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
package org.apache.spark.sql.connect.execution

import java.io.InputStream
import java.util

import scala.util.control.NonFatal

import org.apache.arrow.memory.BufferAllocator

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.{ArrowUtils, CaseInsensitiveStringMap, ConcatenatingArrowStreamReader, MessageIterator}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.storage.CacheId

/**
 * Datasource implementation that can read a Connect Chunked Datasource directly from the
 * block manager.
 *
 * @param id of the LocalRelation.
 * @param sessionId of the LocalRelation.
 * @param schema of the LocalRelation.
 * @param dataHashes ids of the blocks stored in the BlockManager.
 */
class LocalRelationTable(
    id: Long,
    sessionId: String,
    schema: StructType,
    dataHashes: Seq[String])
  extends Table with SupportsRead {

  override def name(): String = s"LocalRelation[session=$sessionId, id=$id]"

  override def columns(): Array[Column] = schema.fields.map { field =>
    Column.create(field.name, field.dataType, field.nullable)
  }

  override def capabilities(): util.Set[TableCapability] = util.EnumSet.of(
    TableCapability.BATCH_READ,
    TableCapability.MICRO_BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new ScanBuilder {
    override def build(): Scan = new LocalRelationScan(schema, sessionId, dataHashes)
  }
}

case class LocalRelationScanPartition(sessionId: String, dataHash: String) extends InputPartition

class LocalRelationScan(schema: StructType, sessionId: String, dataHashes: Seq[String])
  extends Scan with Batch {
  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] =
    dataHashes.map(hash => LocalRelationScanPartition(sessionId, hash)).toArray

  override def createReaderFactory(): PartitionReaderFactory =
    LocalRelationScanPartitionReaderFactory

  override def columnarSupportMode(): Scan.ColumnarSupportMode =
    Scan.ColumnarSupportMode.SUPPORTED
}

object LocalRelationScanPartitionReaderFactory extends PartitionReaderFactory {
  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw new IllegalStateException("Row based reads are not supported (yet)")

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val localRelationScanPartition = partition.asInstanceOf[LocalRelationScanPartition]
    new LocalRelationPartitionReader(localRelationScanPartition)
  }
}

class LocalRelationPartitionReader(partition: LocalRelationScanPartition)
  extends PartitionReader[ColumnarBatch] with Logging {
  private var input: InputStream = _
  private var allocator: BufferAllocator = _
  private var reader: ConcatenatingArrowStreamReader = _

  private def init(): Unit = {
    if (reader == null) {
      val blockManager = SparkEnv.get.blockManager
      val blockId = CacheId(partition.sessionId, partition.dataHash)
      input = blockManager.getLocalBytes(blockId).map(_.toInputStream())
        .orElse(blockManager.getRemoteBytes(blockId).map(_.toInputStream(dispose = true)))
        .getOrElse(throw new SparkException(s"Cannot retrieve $blockId"))
      allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"LocalRelationPartitionReader[$blockId]",
        0,
        Long.MaxValue)
      reader = new ConcatenatingArrowStreamReader(
        allocator,
        Iterator.single(new MessageIterator(input, allocator)),
        destructive = true)
    }
  }

  override def next(): Boolean = {
    init()
    reader.loadNextBatch()
  }

  override def get(): ColumnarBatch = {
    val root = reader.getVectorSchemaRoot
    val columns = root.getFieldVectors.stream().map { vector =>
      new ArrowColumnVector(vector)
    }.toArray(new Array[ColumnVector](_))
    val batch = new ColumnarBatch(columns)
    batch.setNumRows(root.getRowCount)
    batch
  }

  override def close(): Unit = {
    close(reader)
    close(allocator)
    close(input)
  }

  private def close(closeable: AutoCloseable): Unit = {
    if (closeable != null) {
      try {
        closeable.close()
      } catch {
        case NonFatal(e) =>
          logError("Error thrown while close()", e)
      }
    }
  }
}
