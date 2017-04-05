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

package org.apache.spark.util.collection

import java.io._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.google.common.io.ByteStreams

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.serializer.{DeserializationStream, SerializerInstance}
import org.apache.spark.SparkEnv
import org.apache.spark.storage.{BlockId, BlockManager}

private[spark] class SizeTrackingCompactBuffer[T: ClassTag]
  extends CompactBuffer[T] with SizeTracker {

  override def +=(value: T): SizeTrackingCompactBuffer[T] = {
    super.+=(value)
    afterUpdate()
    this
  }

  override def growToSize(newSize: Int): Unit = {
    super.growToSize(newSize)
    resetSamples()
  }
}

/**
 * A data set that is not empty.
 *
 * @param buffer
 * @tparam T
 */
private[spark] class Chunk[T: ClassTag](buffer: CompactBuffer[T]) extends Iterable[T] {
  require(buffer.nonEmpty, "Chunk must contain some data")

  override def iterator: Iterator[T] = buffer.iterator
}

private[spark] class ChunkParameters {
  val sparkConf = SparkEnv.get.conf
  val serializer = SparkEnv.get.serializer
  val blockManager = SparkEnv.get.blockManager
  val diskBlockManager = blockManager.diskBlockManager

  val serializerBatchSize = sparkConf.getLong("spark.join.spill.batchSize", 10000)
  val fileBufferSize = sparkConf.getInt("spark.shuffle.file.buffer.kb", 32) * 1024
}

/**
 * An append-only buffer that spills contents to disk when there is insufficient space for it
 * to grow. If there is insufficient space, it will flush the current contents in memory to disk
 * and further contents will be written to disk directly.
 *
 * The buffer will prevent further append operations once someone starts to read the contents.
 *
 * @param parameters
 * @tparam T
 */
class ChunkBuffer[T: ClassTag](parameters: ChunkParameters)
  extends Iterable[Chunk[T]] with Spillable[CompactBuffer[T]] {

  private var buffer = new SizeTrackingCompactBuffer[T]()

  private var diskBuffer: DiskChunkBuffer[T] = null

  def diskBytesSpilled: Long = if (diskBuffer == null) 0 else diskBuffer.diskBytesSpilled

  def +=(value: T): ChunkBuffer[T] = {
    if (diskBuffer != null) {
      diskBuffer += value
    }
    else {
      buffer += value
      addElementsRead()
      if (maybeSpill(buffer, buffer.estimateSize())) {
        // It's important to release the reference to free the memory
        buffer = null
      }
    }
    this
  }

  def ++=(values: Iterable[T]): ChunkBuffer[T] = {
    for (v <- values.iterator) {
      this += v
    }
    this
  }

  override def iterator: Iterator[Chunk[T]] = {
    if (diskBuffer == null) {
      if (buffer.isEmpty) {
        Iterator()
      }
      else {
        Iterator(new Chunk(buffer))
      }
    }
    else {
      diskBuffer.iterator
    }
  }

  override protected def spill(buffer: CompactBuffer[T]): Unit = {
    diskBuffer = new DiskChunkBuffer[T](parameters)
    buffer.foreach(diskBuffer += _)
  }

  /**
   * @return the size of chunks, or 0 if **no data**.
   */
  override def size: Int = if (diskBuffer == null) {
    if (buffer.isEmpty) 0 else 1
  } else {
    diskBuffer.size
  }
}

/**
 * A buffer to append values to the disk file.
 *
 * @param parameters
 * @tparam T
 */
private[spark] class DiskChunkBuffer[T: ClassTag](parameters: ChunkParameters)
  extends Iterable[Chunk[T]] {

  // If DiskChunkBuffer is frozen, it cannot be modified any more.
  private var isFrozen = false

  // If DiskChunkBuffer becomes corrupt (such as throw some exception), should prevent any further
  // operation
  private var corrupt = false

  private val batchSizes = ArrayBuffer[Long]()

  private val (blockId, file) = parameters.diskBlockManager.createTempLocalBlock()

  private var curWriteMetrics = new ShuffleWriteMetrics()

  private val blockManager = parameters.blockManager
  private val serializer = parameters.serializer
  private val serializerBatchSize = parameters.serializerBatchSize

  private var writer = getWriter()

  private val ser = serializer.newInstance()

  private var _diskBytesSpilled = 0L

  def diskBytesSpilled: Long = _diskBytesSpilled

  private def getWriter() = {
    blockManager.getDiskWriter(blockId, file, serializer, parameters.fileBufferSize,
      curWriteMetrics)
  }

  private var objectsWritten = 0

  def +=(value: T): DiskChunkBuffer[T] = {
    if (corrupt) {
      throw new IllegalStateException("DiskChunkBuffer has been corrupt")
    }

    try {
      if (isFrozen) {
        throw new IllegalStateException("DiskChunkBuffer has been frozen")
      }

      writer.write(value)
      objectsWritten += 1

      if (objectsWritten == serializerBatchSize) {
        flush()
        curWriteMetrics = new ShuffleWriteMetrics()
        writer = getWriter()
      }
      this
    } catch {
      case e: Throwable =>
        corrupt = true
        cleanupOnThrowable()
        throw e
    }
  }

  override def iterator: Iterator[Chunk[T]] = {
    if (corrupt) {
      throw new IllegalStateException("DiskChunkBuffer has been corrupt")
    }

    try {
      if (!isFrozen) {
        frozen()
      }
      new DiskChunkIterator(file, blockId, batchSizes, serializerBatchSize, blockManager, ser)
    } catch {
      case e: Throwable =>
        corrupt = true
        cleanupOnThrowable()
        throw e
    }
  }

  private def frozen(): Unit = {
    isFrozen = true
    if (objectsWritten > 0) {
      flush()
    } else if (writer != null) {
      val w = writer
      writer = null
      w.revertPartialWritesAndClose()
    }
  }

  private def flush(): Unit = {
    val w = writer
    writer = null
    w.commitAndClose()
    _diskBytesSpilled += curWriteMetrics.shuffleBytesWritten
    batchSizes.append(curWriteMetrics.shuffleBytesWritten)
    objectsWritten = 0
  }

  /**
   * Clean up the resources when some exception happens
   */
  private def cleanupOnThrowable(): Unit = {
    if (writer != null) {
      writer.revertPartialWritesAndClose()
      writer == null
    }
    if (file.exists()) {
      file.delete()
    }
  }

  override def size: Int = batchSizes.size
}

object ChunkBuffer {
  def apply[T: ClassTag](parameters: ChunkParameters): ChunkBuffer[T] = {
    new ChunkBuffer(parameters)
  }
}

private[spark] class DiskChunkIterator[T: ClassTag](file: File, blockId: BlockId,
    batchSizes: ArrayBuffer[Long], serializerBatchSize: Long, blockManager: BlockManager,
    ser: SerializerInstance)
  extends Iterator[Chunk[T]] {

  private val batchOffsets = batchSizes.scanLeft(0L)(_ + _) // Size will be batchSize.length + 1
  assert(file.length() == batchOffsets.last,
    "File length is not equal to the last batch offset:\n" +
      s"    file length = ${file.length}\n" +
      s"    last batch offset = ${batchOffsets.last}\n" +
      s"    all batch offsets = ${batchOffsets.mkString(",")}"
  )

  private var nextChunk: Chunk[T] = null

  private var batchIndex = 0 // Which batch we're in

  override def hasNext: Boolean = {
    if (nextChunk == null) {
      nextChunk = moveToNextChunk()
    }
    nextChunk != null
  }

  override def next(): Chunk[T] = {
    if (hasNext) {
      val chunk = nextChunk
      nextChunk = null
      chunk
    }
    else {
      throw new NoSuchElementException
    }
  }

  private def moveToNextChunk(): Chunk[T] = {
    // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
    // we're still in a valid batch.
    if (batchIndex < batchOffsets.length - 1) {
      val buffer = CompactBuffer[T]()
      var objectsRead = 0
      val fileStream = new FileInputStream(file)
      var deserializeStream: DeserializationStream = null
      try {
        val start = batchOffsets(batchIndex)
        fileStream.getChannel.position(start)
        batchIndex += 1

        val end = batchOffsets(batchIndex)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
        val compressedStream = blockManager.wrapForCompression(blockId, bufferedStream)
        deserializeStream = ser.deserializeStream(compressedStream)
        while (objectsRead < serializerBatchSize) {
          buffer += deserializeStream.readObject()
          objectsRead += 1
        }
        new Chunk(buffer)
      } finally {
        deserializeStream.close()
        fileStream.close()
      }
    } else {
      // No more batches left, so delete the file
      // TODO how to delete the file if any exception happens when using the iterator
      if (file.exists()) {
        file.delete()
      }
      null
    }
  }
}
