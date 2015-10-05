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

import java.io.{EOFException, BufferedInputStream, FileInputStream, File}

import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.serializer.{DeserializationStream, Serializer}
import org.apache.spark.storage.{DiskBlockManager, BlockId, DiskBlockObjectWriter, BlockManager}
import org.apache.spark.util.collection.SpillableCollection._

/**
 *
 * Collection that can spill to disk. Takes type parameters T, the iterable type, and
 * C, the type of the elements returned by T's iterator.
 */
private[spark] trait SpillableCollection[C, T <: Iterable[C]] extends Spillable[T] {
  // Write metrics for current spill
  private var curWriteMetrics: ShuffleWriteMetrics = _
  // Number of bytes spilled in total
  protected var _diskBytesSpilled = 0L
  private lazy val ser = serializer.newInstance()

  def diskBytesSpilled: Long = _diskBytesSpilled

  override protected final def spill(collection: T): Unit = {
    val (blockId, file) = diskBlockManager.createTempLocalBlock()
    curWriteMetrics = new ShuffleWriteMetrics()
    var writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, curWriteMetrics)
    var objectsWritten = 0

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // Flush the disk writer's contents to disk, and update relevant variables
    def flush(): Unit = {
      val w = writer
      writer = null
      w.commitAndClose()
      _diskBytesSpilled += curWriteMetrics.shuffleBytesWritten
      batchSizes.append(curWriteMetrics.shuffleBytesWritten)
      objectsWritten = 0
    }

    var success = false
    try {
      val it = getIteratorForCurrentSpillable()
      while (it.hasNext) {
        val kv = it.next()
        writeNextObject(kv, writer)
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {
          flush()
          curWriteMetrics = new ShuffleWriteMetrics()
          writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, curWriteMetrics)
        }
      }
      if (objectsWritten > 0) {
        flush()
      } else if (writer != null) {
        val w = writer
        writer = null
        w.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (!success) {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        if (writer != null) {
          writer.revertPartialWritesAndClose()
        }
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }
    recordNextSpilledPart(file, blockId, batchSizes)
  }


  protected def getIteratorForCurrentSpillable(): Iterator[C]
  protected def writeNextObject(c: C, writer: DiskBlockObjectWriter): Unit
  protected def recordNextSpilledPart(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])

  /**
   * Iterator backed by elements from batches on disk.
   */
  protected abstract class DiskIterator(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
      extends Iterator[C] {
    private val batchOffsets = batchSizes.scanLeft(0L)(_ + _)  // Size will be batchSize.length + 1
    assert(file.length() == batchOffsets.last,
      "File length is not equal to the last batch offset:\n" +
        s"    file length = ${file.length}\n" +
        s"    last batch offset = ${batchOffsets.last}\n" +
        s"    all batch offsets = ${batchOffsets.mkString(",")}"
    )

    private var batchIndex = 0  // Which batch we're in
    private var fileStream: FileInputStream = null

    // An intermediate stream that reads from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    private var deserializeStream = nextBatchStream()
    private var nextItem: Option[C] = None
    private var objectsRead = 0

    /**
     * Construct a stream that reads only from the next batch.
     */
    protected def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      if (batchIndex < batchOffsets.length - 1) {
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        val start = batchOffsets(batchIndex)
        fileStream = new FileInputStream(file)
        fileStream.getChannel.position(start)
        batchIndex += 1

        val end = batchOffsets(batchIndex)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
        val compressedStream = blockManager.wrapForCompression(blockId, bufferedStream)
        ser.deserializeStream(compressedStream)
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    /**
     * Return the next item from the deserialization stream.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more items are left, return null.
     */
    protected def readNextItem(): Option[C] = {
      try {
        val item = readNextItemFromStream(deserializeStream)
        objectsRead += 1
        if (objectsRead == serializerBatchSize) {
          objectsRead = 0
          deserializeStream = nextBatchStream()
        }
        Some(item)
      } catch {
        case e: EOFException =>
          cleanup()
          None
      }
    }

    private def cleanup() {
      batchIndex = batchOffsets.length // Prevent reading any other batch
      val ds = deserializeStream
      deserializeStream = null
      if (ds != null) {
        ds.close()
      }
      val fs = fileStream
      fileStream = null
      if (fs != null) {
        fs.close()
      }
      if (shouldCleanupFileAfterOneIteration()) {
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }

    override def hasNext(): Boolean = {
      if (!nextItem.isDefined) {
        if (deserializeStream == null) {
          return false
        }
        nextItem = readNextItem()
      }
      nextItem.isDefined
    }

    override def next(): C = {
      if (!hasNext()) {
        throw new NoSuchElementException()
      }
      val nextValue = nextItem.get
      nextItem = None
      nextValue
    }

    protected def readNextItemFromStream(deserializeStream: DeserializationStream): C
    protected def shouldCleanupFileAfterOneIteration(): Boolean
  }
}

private object SpillableCollection {
  private def sparkConf(): SparkConf = SparkEnv.get.conf
  private def blockManager(): BlockManager = SparkEnv.get.blockManager
  private def diskBlockManager(): DiskBlockManager = blockManager.diskBlockManager
  private def fileBufferSize(): Int =
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    sparkConf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024
  /**
   * Size of object batches when reading/writing from serializers.
   *
   * Objects are written in batches, with each batch using its own serialization stream. This
   * cuts down on the size of reference-tracking maps constructed when deserializing a stream.
   *
   * NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
   * grow internal data structures by growing + copying every time the number of objects doubles.
   */
  private def serializerBatchSize(): Long =
    sparkConf.getLong("spark.shuffle.spill.batchSize", 10000)

  private def serializer(): Serializer = SparkEnv.get.serializer
}
