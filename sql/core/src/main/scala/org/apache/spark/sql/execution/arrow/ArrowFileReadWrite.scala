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

import java.io.{ByteArrayOutputStream, FileOutputStream}
import java.nio.channels.Channels
import java.nio.file.Files
import java.nio.file.Paths

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowFileWriter, WriteChannel}
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.util.ArrowUtils

private[sql] class SparkArrowFileWriter(
  arrowSchema: Schema,
  path: String) extends AutoCloseable {
  private val allocator =
    ArrowUtils.rootAllocator.newChildAllocator(
      s"to${this.getClass.getSimpleName}", 0, Long.MaxValue)

  protected val root = VectorSchemaRoot.create(arrowSchema, allocator)
  protected val loader = new VectorLoader(root)
  protected val arrowWriter = ArrowWriter.create(root)

  protected val fileWriter =
    new ArrowFileWriter(root, null, Channels.newChannel(new FileOutputStream(path)))

  override def close(): Unit = {
    root.close()
    allocator.close()
    fileWriter.close()
  }

  def write(batchBytesIter: Iterator[Array[Byte]]): Unit = {
    fileWriter.start()
    while (batchBytesIter.hasNext) {
      val batchBytes = batchBytesIter.next()
      val batch = ArrowConverters.loadBatch(batchBytes, allocator)
      loader.load(batch)
      fileWriter.writeBatch()
    }
    fileWriter.close()
  }
}

private[sql] class SparkArrowFileReader(path: String) extends AutoCloseable {
  private val allocator =
    ArrowUtils.rootAllocator.newChildAllocator(
      s"to${this.getClass.getSimpleName}", 0, Long.MaxValue)

  protected val fileReader =
    new ArrowFileReader(Files.newByteChannel(Paths.get(path)), allocator)

  override def close(): Unit = {
    allocator.close()
    fileReader.close()
  }

  val schema: Schema = fileReader.getVectorSchemaRoot.getSchema

  def read(): Iterator[Array[Byte]] = {
    fileReader.getRecordBlocks.iterator().asScala.map { block =>
      fileReader.loadRecordBatch(block)
      val root = fileReader.getVectorSchemaRoot
      val unloader = new VectorUnloader(root)
      val batch = unloader.getRecordBatch
      val out = new ByteArrayOutputStream()
      val writeChannel = new WriteChannel(Channels.newChannel(out))
      MessageSerializer.serialize(writeChannel, batch)
      out.toByteArray
    }
  }
}

private[spark] object ArrowFileReadWrite {
  def save(df: DataFrame, path: String): Unit = {
    val maxRecordsPerBatch = df.sparkSession.sessionState.conf.arrowMaxRecordsPerBatch
    val rdd = df.toArrowBatchRdd(maxRecordsPerBatch, "UTC", true, false)
    val arrowSchema = ArrowUtils.toArrowSchema(df.schema, "UTC", true, false)
    val writer = new SparkArrowFileWriter(arrowSchema, path)
    writer.write(rdd.toLocalIterator)
  }

  def load(spark: SparkSession, path: String): DataFrame = {
    val reader = new SparkArrowFileReader(path)
    val schema = ArrowUtils.fromArrowSchema(reader.schema)
    ArrowConverters.toDataFrame(reader.read(), schema, spark, "UTC", true, false)
  }
}
