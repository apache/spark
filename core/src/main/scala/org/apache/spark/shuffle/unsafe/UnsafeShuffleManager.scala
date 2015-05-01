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

package org.apache.spark.shuffle.unsafe

import java.io.{ByteArrayOutputStream, FileOutputStream}
import java.nio.ByteBuffer
import java.util

import com.esotericsoftware.kryo.io.ByteBufferOutputStream
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.unsafe.PlatformDependent
import org.apache.spark.unsafe.memory.{TaskMemoryManager, MemoryBlock}
import org.apache.spark.unsafe.sort.UnsafeSorter
import org.apache.spark.unsafe.sort.UnsafeSorter.{KeyPointerAndPrefix, PrefixComparator, PrefixComputer, RecordComparator}
import org.apache.spark.{SparkEnv, SparkConf, ShuffleDependency, TaskContext}
import org.apache.spark.shuffle._

private[spark] class UnsafeShuffleHandle[K, V](
    shuffleId: Int,
    override val numMaps: Int,
    override val dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
  require(UnsafeShuffleManager.canUseUnsafeShuffle(dependency))
}

private[spark] object UnsafeShuffleManager {
  def canUseUnsafeShuffle[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = {
    dependency.aggregator.isEmpty && dependency.keyOrdering.isEmpty
  }
}

private object DummyRecordComparator extends RecordComparator {
  override def compare(
      leftBaseObject: scala.Any,
      leftBaseOffset: Long,
      rightBaseObject: scala.Any,
      rightBaseOffset: Long): Int = {
    0
  }
}

private object PartitionerPrefixComputer extends PrefixComputer {
  override def computePrefix(baseObject: scala.Any, baseOffset: Long): Long = {
    // TODO: should the prefix be computed when inserting the record pointer rather than being
    // read from the record itself?  May be more efficient in terms of space, etc, and is a simple
    // change.
    PlatformDependent.UNSAFE.getLong(baseObject, baseOffset)
  }
}

private object PartitionerPrefixComparator extends PrefixComparator {
  override def compare(prefix1: Long, prefix2: Long): Int = {
    (prefix1 - prefix2).toInt
  }
}

private[spark] class UnsafeShuffleWriter[K, V](
    shuffleBlockManager: IndexShuffleBlockManager,
    handle: UnsafeShuffleHandle[K, V],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] {

  println("Construcing a new UnsafeShuffleWriter")

  private[this] val memoryManager: TaskMemoryManager = context.taskMemoryManager()

  private[this] val dep = handle.dependency

  private[this] var sorter: UnsafeSorter = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private[this] var stopping = false

  private[this] var mapStatus: MapStatus = null

  private[this] val writeMetrics = new ShuffleWriteMetrics()
  context.taskMetrics().shuffleWriteMetrics = Some(writeMetrics)

  private[this] val allocatedPages: util.LinkedList[MemoryBlock] =
    new util.LinkedList[MemoryBlock]()

  private[this] val blockManager = SparkEnv.get.blockManager

  /** Write a sequence of records to this task's output */
  override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    println("Opened writer!")
    val serializer = Serializer.getSerializer(dep.serializer).newInstance()
    val partitioner = dep.partitioner
    sorter = new UnsafeSorter(
      context.taskMemoryManager(),
      DummyRecordComparator,
      PartitionerPrefixComputer,
      PartitionerPrefixComparator,
      4096  // initial size
    )

    // Pack records into data pages:
    val PAGE_SIZE = 1024 * 1024 * 1
    var currentPage: MemoryBlock = memoryManager.allocatePage(PAGE_SIZE)
    allocatedPages.add(currentPage)
    var currentPagePosition: Long = currentPage.getBaseOffset

    // TODO make this configurable
    val serArray = new Array[Byte](1024 * 1024)
    val byteBuffer = ByteBuffer.wrap(serArray)
    val bbos = new ByteBufferOutputStream()
    bbos.setByteBuffer(byteBuffer)
    val serBufferSerStream = serializer.serializeStream(bbos)

    while (records.hasNext) {
      val nextRecord: Product2[K, V] = records.next()
      println("Writing record " + nextRecord)
      val partitionId: Int = partitioner.getPartition(nextRecord._1)
      serBufferSerStream.writeObject(nextRecord)

      val sizeRequirement: Int = byteBuffer.position() + 8 + 8
      println("Size requirement in intenral buffer is " + sizeRequirement)
      if (sizeRequirement > (PAGE_SIZE - currentPagePosition)) {
        println("Allocating a new data page after writing " + currentPagePosition)
        currentPage = memoryManager.allocatePage(PAGE_SIZE)
        allocatedPages.add(currentPage)
        currentPagePosition = currentPage.getBaseOffset
      }
      println("Before writing record, current page position is " + currentPagePosition)
      // TODO: check that it's still not too large
      val newRecordAddress =
        memoryManager.encodePageNumberAndOffset(currentPage, currentPagePosition)
      PlatformDependent.UNSAFE.putLong(currentPage.getBaseObject, currentPagePosition, partitionId)
      currentPagePosition += 8
      PlatformDependent.UNSAFE.putLong(
        currentPage.getBaseObject, currentPagePosition, byteBuffer.position())
      currentPagePosition += 8
      PlatformDependent.copyMemory(
        serArray,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        currentPage.getBaseObject,
        currentPagePosition,
        byteBuffer.position())
      currentPagePosition += byteBuffer.position()
      println("After writing record, current page position is " + currentPagePosition)
      sorter.insertRecord(newRecordAddress)
      byteBuffer.position(0)
    }
    // TODO: free the buffers, etc, at this point since they're not needed
    val sortedIterator: util.Iterator[KeyPointerAndPrefix] = sorter.getSortedIterator
    // Now that the partition is sorted, write out the data to a file, keeping track off offsets
    // for use in the sort-based shuffle index.
    val outputFile = shuffleBlockManager.getDataFile(dep.shuffleId, mapId)
    val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockManager.NOOP_REDUCE_ID)
    val partitionLengths = new Array[Long](partitioner.numPartitions)
    // TODO: compression tests?
    // TODO why is append true here?
    // TODO: metrics tracking and all of the other stuff that diskblockobjectwriter would give us
    // TODO: note that we saw FAILED_TO_UNCOMPRESS(5) at some points during debugging when we were
    // not properly wrapping the writer for compression even though readers expected compressed
    // data; the fact that someone still reported this issue in newer Spark versions suggests that
    // we should audit the code to make sure wrapping is done at the right set of places and to
    // check that we haven't missed any rare corner-cases / rarely-used paths.
    val out = blockManager.wrapForCompression(blockId, new FileOutputStream(outputFile, true))
    val serOut = serializer.serializeStream(out)
    serOut.flush()
    var currentPartition = -1
    var currentPartitionLength: Long = 0
    while (sortedIterator.hasNext) {
      val keyPointerAndPrefix: KeyPointerAndPrefix = sortedIterator.next()
      val partition = keyPointerAndPrefix.keyPrefix.toInt
      println("Partition is " + partition)
      if (currentPartition == -1) {
        currentPartition = partition
      }
      if (partition != currentPartition) {
        println("switching partition")
        partitionLengths(currentPartition) = currentPartitionLength
        currentPartitionLength = 0
        currentPartition = partition
      }
      val baseObject = memoryManager.getPage(keyPointerAndPrefix.recordPointer)
      val baseOffset = memoryManager.getOffsetInPage(keyPointerAndPrefix.recordPointer)
      val recordLength = PlatformDependent.UNSAFE.getLong(baseObject, baseOffset + 8)
      partitionLengths(currentPartition) += recordLength
      println("Base offset is " + baseOffset)
      println("Record length is " + recordLength)
      var i: Int = 0
      // TODO: need to have a way to figure out whether a serializer supports relocation of
      // serialized objects or not.  Sandy also ran into this in his patch (see
      // https://github.com/apache/spark/pull/4450).  If we're using Java serialization, we might
      // as well just bypass this optimized code path in favor of the old one.
      while (i < recordLength) {
        out.write(PlatformDependent.UNSAFE.getByte(baseObject, baseOffset + 16 + i))
        i += 1
      }
    }
    out.flush()
    //serOut.close()
    //out.flush()
    out.close()
    shuffleBlockManager.writeIndexFile(dep.shuffleId, mapId, partitionLengths)
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    println("Stopping unsafeshufflewriter")
    try {
      if (stopping) {
        None
      } else {
        stopping = true
        if (success) {
          Option(mapStatus)
        } else {
          // The map task failed, so delete our output data.
          shuffleBlockManager.removeDataByMap(dep.shuffleId, mapId)
          None
        }
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val iter = allocatedPages.iterator()
        while (iter.hasNext) {
          memoryManager.freePage(iter.next())
          iter.remove()
        }
        val startTime = System.nanoTime()
        //sorter.stop()
        context.taskMetrics().shuffleWriteMetrics.foreach(
          _.incShuffleWriteTime(System.nanoTime - startTime))
        sorter = null
      }
    }
  }
}



private[spark] class UnsafeShuffleManager(conf: SparkConf) extends ShuffleManager {

  private[this] val sortShuffleManager: SortShuffleManager = new SortShuffleManager(conf)

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (UnsafeShuffleManager.canUseUnsafeShuffle(dependency)) {
      println("Opening unsafeShuffleWriter")
      new UnsafeShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    sortShuffleManager.getReader(handle, startPartition, endPartition, context)
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext): ShuffleWriter[K, V] = {
    handle match {
      case unsafeShuffleHandle: UnsafeShuffleHandle[K, V] =>
        // TODO: do we need to do anything to register the shuffle here?
        new UnsafeShuffleWriter(
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockManager],
          unsafeShuffleHandle,
          mapId,
          context)
      case other =>
        sortShuffleManager.getWriter(handle, mapId, context)
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    // TODO: need to do something here for our unsafe path
    sortShuffleManager.unregisterShuffle(shuffleId)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    sortShuffleManager.shuffleBlockResolver
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    sortShuffleManager.stop()
  }
}
