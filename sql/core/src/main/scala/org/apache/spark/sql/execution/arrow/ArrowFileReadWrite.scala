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

package org.apache.spark.sql.execution.arrow

import java.nio.channels.Channels
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowFileWriter}
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.util.ArrowUtils

private[sql] class SparkArrowFileWriter(schema: Schema, path: Path) extends AutoCloseable {
  private val allocator = ArrowUtils.rootAllocator
    .newChildAllocator(s"to${this.getClass.getSimpleName}", 0, Long.MaxValue)

  protected val root = VectorSchemaRoot.create(schema, allocator)
  protected val loader = new VectorLoader(root)

  protected val fileWriter =
    new ArrowFileWriter(root, null, Channels.newChannel(Files.newOutputStream(path)))

  // AtomicBoolean ensures close() is safe to call concurrently and from multiple code paths
  // (write()'s finally block and the outer save() finally block). ArrowFileWriter.close() is
  // NOT idempotent, so we must guard it; root/allocator are closed unconditionally via
  // try/finally so they are always released even if fileWriter.close() throws.
  private val fileWriterClosed = new AtomicBoolean(false)

  override def close(): Unit = {
    try {
      if (fileWriterClosed.compareAndSet(false, true)) fileWriter.close()
    } finally {
      try { root.close() } finally { allocator.close() }
    }
  }

  def write(batchBytesIter: Iterator[Array[Byte]]): Unit = {
    fileWriter.start()
    try {
      while (batchBytesIter.hasNext) {
        val batchBytes = batchBytesIter.next()
        val batch = ArrowConverters.loadBatch(batchBytes, allocator)
        try {
          loader.load(batch)
          fileWriter.writeBatch()
        } finally {
          batch.close()
        }
      }
    } finally {
      // close() uses compareAndSet so this is safe even if the outer save() finally also calls
      // writer.close()
      if (fileWriterClosed.compareAndSet(false, true)) fileWriter.close()
    }
  }
}

private[sql] class SparkArrowFileReader(path: Path) extends AutoCloseable {
  private val allocator = ArrowUtils.rootAllocator
    .newChildAllocator(s"to${this.getClass.getSimpleName}", 0, Long.MaxValue)

  protected val fileReader =
    new ArrowFileReader(Files.newByteChannel(path), allocator)

  override def close(): Unit = {
    fileReader.close()
    allocator.close()
  }

  val schema: Schema = fileReader.getVectorSchemaRoot.getSchema

  def read(): Iterator[Array[Byte]] = {
    fileReader.getRecordBlocks.iterator().asScala.map { block =>
      fileReader.loadRecordBatch(block)
      val root = fileReader.getVectorSchemaRoot
      val unloader = new VectorUnloader(root)
      val batch = unloader.getRecordBatch
      val bytes = ArrowConverters.serializeBatch(batch)
      batch.close()
      bytes
    }
  }
}

private[spark] object ArrowFileReadWrite {
  def save(df: DataFrame, path: Path): Unit = {
    val maxRecordsPerBatch = df.sparkSession.sessionState.conf.arrowMaxRecordsPerBatch
    val rdd = df.toArrowBatchRdd(maxRecordsPerBatch, "UTC", true, false)
    val arrowSchema = ArrowUtils.toArrowSchema(df.schema, "UTC", true, false)
    val writer = new SparkArrowFileWriter(arrowSchema, path)
    try {
      writer.write(rdd.toLocalIterator)
    } finally {
      writer.close()
    }
  }

  def load(spark: SparkSession, path: Path): DataFrame = {
    val reader = new SparkArrowFileReader(path)
    try {
      val schema = ArrowUtils.fromArrowSchema(reader.schema)
      val batches = reader.read().toArray
      ArrowConverters.toDataFrame(batches.iterator, schema, spark, "UTC", true, false)
    } finally {
      reader.close()
    }
  }
}
