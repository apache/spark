package org.apache.spark.util.collection

import java.io._
import java.util.Comparator

import scala.collection.BufferedIterator
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.google.common.io.ByteStreams

import org.apache.spark.{Logging, SparkEnv, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.serializer.{DeserializationStream, Serializer}
import org.apache.spark.storage.{BlockId, BlockManager}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalAppendOnlyMap.HashComparator
import org.apache.spark.executor.ShuffleWriteMetrics

class ExternalAppendOnlyArrayBuffer[T: ClassTag](
    serializer: Serializer = SparkEnv.get.serializer,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    context: TaskContext = TaskContext.get())
  extends Iterable[T]
  with Serializable
  with Logging
  with Spillable[SizeTracker] {
  if (context == null) {
    throw new IllegalStateException(
      "Spillable collections should not be instantiated outside of tasks")
  }

  private var currentArray = new SizeTrackingVector[T]
  private var spilledArrays = new ArrayBuffer[DiskArrayIterator]
  private val arrayBuffered: ArrayBuffer[T] = new ArrayBuffer[T]
  private val sparkConf = SparkEnv.get.conf
  private val diskBlockManager = blockManager.diskBlockManager

  private val serializerBatchSize = sparkConf.getLong("spark.shuffle.spill.batchSize", 10000)
  private val cacheSizeInArrayBuffer = sparkConf.getInt("spark.cache.array.size", 10000)

  // Number of bytes spilled in total
  private var _diskBytesSpilled = 0L
  def diskBytesSpilled: Long = _diskBytesSpilled

  // Write metrics for current spill
  private var curWriteMetrics: ShuffleWriteMetrics = _
  private var _size: Int = 0
  private var _getsize: Int = 0

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val fileBufferSize =
    sparkConf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  private val ser = serializer.newInstance()

  def length: Int = _size

  override def reset(): Unit = {
    logDebug("Reset:" + spilledArrays.length + " currentArray.size:" + currentArray.size)
    super.reset()
    if (spilledArrays.length > 0) {
      _diskBytesSpilled = 0
      spilledArrays.map(iter => iter.deleteTmpFile)
      spilledArrays = new ArrayBuffer[DiskArrayIterator]
    }
    if (currentArray.size > 0) {
      currentArray = new SizeTrackingVector[T]
    }
    _size = 0
    arrayBuffered.clear
    releaseMemory()
  }

  override protected[this] def taskMemoryManager: TaskMemoryManager = context.taskMemoryManager()

  def +=(elem: T): Unit = {
    _size += 1
    addElementsRead()
    if (_size < cacheSizeInArrayBuffer) {
      arrayBuffered += elem
      return
    }
    if (arrayBuffered.length > 0) {
      val iter = arrayBuffered.iterator
      while(iter.hasNext) {
        currentArray += iter.next
      }
      arrayBuffered.clear
    }
    if(currentArray.length % 100000 == 0) {
      logInfo("currentArray.len:" + currentArray.length)
    }
    val estimatedSize = currentArray.estimateSize()
    if (maybeSpill(currentArray, estimatedSize)) {
      currentArray = new SizeTrackingVector[T]
    }
    currentArray += elem
  }

  override protected[this] def spill(collection: SizeTracker): Unit = {
    val (blockId, file) = diskBlockManager.createTempLocalBlock()
    logInfo("Spill len:" + currentArray.length + " file:" + file)
    curWriteMetrics = new ShuffleWriteMetrics()
    var writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, curWriteMetrics)
    var objectsWritten = 0

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
      val it = currentArray.iterator
      while (it.hasNext) {
        val elem = it.next()
        writer.write(null, elem)
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
    spilledArrays.append(new DiskArrayIterator(file, blockId, batchSizes))
  }

  override def iterator: Iterator[T] = {
    logInfo("Match size:" + _size)
    if (_size < cacheSizeInArrayBuffer) {
      arrayBuffered.iterator
    } else if (0 == spilledArrays.length) {
      currentArray.iterator
    } else {
      new ExternalIterator()
    }
  }

  private class ExternalIterator extends Iterator[T] {

    var currentIndex = 0
    var currentIter = spilledArrays(currentIndex)
    val arrayIter = currentArray.iterator
    override def hasNext: Boolean = {
      if (currentIter.hasNext) {
        return true
      } else if (currentIndex < spilledArrays.length - 1) {
        currentIndex += 1
        currentIter = spilledArrays(currentIndex)
        return true
      } else {
        arrayIter.hasNext
      }
    }

    override def next(): T = {
      _getsize += 1
      if (currentIter.hasNext) {
        val tmp: T = currentIter.next
        if (_getsize % 100000 == 0) {
          logInfo("Getsize" + currentIndex + ":" + _getsize)
        }
        tmp
      } else if (currentIndex < spilledArrays.length - 1) {
        currentIndex += 1
        currentIter = spilledArrays(currentIndex)
        _getsize = 0
        val tmp: T = currentIter.next
        if (_getsize % 100000 == 0) {
          logInfo("Getsize" + currentIndex + ":" + _getsize)
        }
        tmp
      } else {
        val tmp: T = arrayIter.next
        if (_getsize % 100000 == 0) {
          logInfo("Getsize array:" + _getsize)
        }
        tmp
      }
    }
  }

  private class DiskArrayIterator(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
    extends Iterator[T] {

    private val batchOffsets = batchSizes.scanLeft(0L)(_ + _)  // Size will be batchSize.length + 1
    logDebug("BatchOffsets:" + batchOffsets.length + " file.length():" + file.length()
        + " batchOffsets.last:" + batchOffsets.last)
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
    private var itemIsNull = true
    private var nextItem: T = _
    private var objectsRead = 0

    /**
     * Construct a stream that reads only from the next batch.
     */
    private def nextBatchStream(): DeserializationStream = {
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
        cleanup(false)
        null
      }
    }

    /**
     * Return the next T pair from the deserialization stream.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more element are left, return null.
     */
    private def readNextItem(): T = {
      try {
        val k = deserializeStream.readKey()
        val c = deserializeStream.readValue().asInstanceOf[T]
        objectsRead += 1
        if (objectsRead == serializerBatchSize) {
          objectsRead = 0
          deserializeStream = nextBatchStream()
        }
        itemIsNull = false
        c
      } catch {
        case e: EOFException =>
          e.printStackTrace()
          cleanup(false)
          itemIsNull = true
          nextItem
      }
    }

    override def hasNext: Boolean = {
      if (itemIsNull) {
        if (deserializeStream == null) {
          return false
        }
        nextItem = readNextItem()
      }
      !itemIsNull
    }

    override def next(): T = {
      val item = if (itemIsNull) readNextItem() else nextItem
      if (itemIsNull) {
        throw new NoSuchElementException
      }
      itemIsNull = true
      item
    }

    private def cleanup(deleteFile: Boolean) {
      batchIndex = batchOffsets.length  // Prevent reading any other batch
      val ds = deserializeStream
      if (ds != null) {
        ds.close()
        deserializeStream = null
      }
      if (fileStream != null) {
        fileStream.close()
        fileStream = null
      }
      if (deleteFile) {
        deleteTmpFile
      }
    }

    def deleteTmpFile() {
      if (file.exists()) {
        if (!file.delete()) {
          logWarning(s"Error deleting ${file}")
        }
      }
    }

    context.addTaskCompletionListener(context => cleanup(true))
  }
}