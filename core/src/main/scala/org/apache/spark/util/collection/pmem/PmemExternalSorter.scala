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

package org.apache.spark.util.collection.pmem

import com.esotericsoftware.kryo.KryoException
import java.io.{ByteArrayInputStream, InputStream}
import java.util.Comparator
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.storage.{BlockId, ShuffleBlockId}
import org.apache.spark.storage.pmem._
import org.apache.spark.util.collection._

private[spark] class PmemExternalSorter[K, V, C](
    context: TaskContext,
    handle: BaseShuffleHandle[K, _, C],
    aggregator: Option[Aggregator[K, V, C]] = None,
    partitioner: Option[Partitioner] = None,
    ordering: Option[Ordering[K]] = None,
    serializer: Serializer = SparkEnv.get.serializer)
  extends ExternalSorter[K, V, C](context, aggregator, partitioner, ordering, serializer)
  with Logging {
  var partitionBufferArray = ArrayBuffer[PmemBlockObjectStream]()
  var mapSideCombine = false
  private val dep = handle.dependency
  private val serializerManager = SparkEnv.get.serializerManager
  private val serInstance = serializer.newInstance()
  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  private val inMemoryCollectionSizeThreshold: Long =
    SparkEnv.get.conf.getLong("spark.shuffle.spill.pmem.MemoryThreshold", 5 * 1024 * 1024)

  private val keyComparator: Comparator[K] = ordering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K): Int = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })

  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || aggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  def setPartitionByteBufferArray(writerArray: Array[PmemBlockObjectStream] = null): Unit = {
    for (i <- 0 until writerArray.length) {
      partitionBufferArray += writerArray(i)
    }
    mapSideCombine = true
  }

  def getPartitionByteBufferArray(stageId: Int, partitionId: Int): PmemBlockObjectStream = {
    if (mapSideCombine) {
      partitionBufferArray(partitionId)
    } else {
      partitionBufferArray += new PmemBlockObjectStream(serializerManager,
        serInstance,
        context.taskMetrics(),
        PmemBlockId.getTempBlockId(stageId),
        SparkEnv.get.conf,
        1,
        numPartitions)
      partitionBufferArray(partitionBufferArray.length - 1)
    }
  }

  def forceSpillToPmem(): Boolean = {
    val usingMap = aggregator.isDefined
    val collection: WritablePartitionedPairCollection[K, C] =
      if (usingMap) getCollection("map") else getCollection("buffer")
    spill(collection)
    true
  }

  override protected[this] def maybeSpill(collection: WritablePartitionedPairCollection[K, C],
                                          currentMemory: Long): Boolean = {
    var shouldSpill = false

    if (elementsRead % 32 == 0 && currentMemory >= inMemoryCollectionSizeThreshold) {
      shouldSpill = currentMemory >= inMemoryCollectionSizeThreshold
    }
    if (shouldSpill) {
      spill(collection)
    }
    shouldSpill
  }

  override protected[this] def spill(
    collection: WritablePartitionedPairCollection[K, C]): Unit = {
    val inMemoryIterator = collection.destructiveSortedWritablePartitionedIterator(comparator)
    spillMemoryIteratorToPmem(inMemoryIterator)
  }

  private[this] def spillMemoryIteratorToPmem(
    inMemoryIterator: WritablePartitionedIterator): Unit = {
    var buffer: PmemBlockObjectStream = null
    var cur_partitionId = -1
    while (inMemoryIterator.hasNext) {
      var partitionId = inMemoryIterator.nextPartition()
      if (cur_partitionId != partitionId) {
        if (cur_partitionId != -1) {
          buffer.maybeSpill(true)
        }
        cur_partitionId = partitionId
        buffer = getPartitionByteBufferArray(dep.shuffleId, cur_partitionId)
      }
      require(partitionId >= 0 && partitionId < numPartitions,
        s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")

      val elem = if (inMemoryIterator.hasNext) inMemoryIterator.writeNext(buffer) else null
    }
    buffer.maybeSpill(true)
  }

  private def groupByPartition(data: Iterator[((Int, K), C)])
  : Iterator[(Int, Iterator[Product2[K, C]])] =
  {
    val buffered = data.buffered
    (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
  }

  private[this] class IteratorForPartition(
    partitionId: Int, data: BufferedIterator[((Int, K), C)]) extends Iterator[Product2[K, C]] {
    var counts: Long = 0
    override def hasNext: Boolean = {
      data.hasNext && data.head._1._1 == partitionId
    }

    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val elem = data.next()
      counts += 1
      (elem._1._2, elem._2)
    }
  }

  def getCollection(variableName: String): WritablePartitionedPairCollection[K, C] = {
    import java.lang.reflect._
    // use reflection to get private map or buffer
    var privateField: Field = this.getClass().getSuperclass().getDeclaredField(variableName)
    privateField.setAccessible(true)
    var fieldValue = privateField.get(this)
    fieldValue.asInstanceOf[WritablePartitionedPairCollection[K, C]]
  }

  override def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    val usingMap = aggregator.isDefined
    val collection: WritablePartitionedPairCollection[K, C] =
      if (usingMap) getCollection("map") else getCollection("buffer")
    if (partitionBufferArray.isEmpty) {
      if (!ordering.isDefined) {
        groupByPartition(destructiveIterator(
          collection.partitionedDestructiveSortedIterator(None)))
      } else {
        groupByPartition(destructiveIterator(
          collection.partitionedDestructiveSortedIterator(Some(keyComparator))))
      }
    } else {
      merge(destructiveIterator(
        collection.partitionedDestructiveSortedIterator(comparator)))
    }
  }

  def merge(inMemory: Iterator[((Int, K), C)]): Iterator[(Int, Iterator[Product2[K, C]])] = {
    // this function is used to merge spilled data with inMemory records
    val inMemBuffered = inMemory.buffered
    val readers = partitionBufferArray.map(partitionBuffer => {new SpillReader(partitionBuffer)})
    (0 until numPartitions).iterator.map { partitionId =>
      val inMemIterator = new IteratorForPartition(partitionId, inMemBuffered)
      val iterators = readers.map(_.readPartitionIter(partitionId)) ++ Seq(inMemIterator)

      if (aggregator.isDefined) {
        (partitionId, mergeWithAggregation(
          iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
      } else if (ordering.isDefined) {
        (partitionId, mergeSort(iterators, ordering.get))
      } else {
        (partitionId, iterators.iterator.flatten)
      }
    }
  }
  /**
   * Merge-sort a sequence of (K, C) iterators using a given a comparator for the keys.
   */
  def mergeSort(iterators: Seq[Iterator[Product2[K, C]]],
                comparator: Comparator[K]): Iterator[Product2[K, C]] = {
    val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
    type Iter = BufferedIterator[Product2[K, C]]
    val heap = new mutable.PriorityQueue[Iter]()(new Ordering[Iter] {
      // Use the reverse order because PriorityQueue dequeues the max
      override def compare(x: Iter, y: Iter): Int = comparator.compare(y.head._1, x.head._1)
    })
    heap.enqueue(bufferedIters: _*)  // Will contain only the iterators with hasNext = true
    new Iterator[Product2[K, C]] {
      override def hasNext: Boolean = !heap.isEmpty

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val firstBuf = heap.dequeue()
        val firstPair = firstBuf.next()
        if (firstBuf.hasNext) {
          heap.enqueue(firstBuf)
        }
        firstPair
      }
    }
  }

  def mergeWithAggregation(
      iterators: Seq[Iterator[Product2[K, C]]],
      mergeCombiners: (C, C) => C,
      comparator: Comparator[K],
      totalOrder: Boolean)
      : Iterator[Product2[K, C]] = {
    if (!totalOrder) {
      // We only have a partial ordering, e.g. comparing the keys by hash code, which means that
      // multiple distinct keys might be treated as equal by the ordering. To deal with this, we
      // need to read all keys considered equal by the ordering at once and compare them.
      new Iterator[Iterator[Product2[K, C]]] {
        val sorted = mergeSort(iterators, comparator).buffered

        // Buffers reused across elements to decrease memory allocation
        val keys = new ArrayBuffer[K]
        val combiners = new ArrayBuffer[C]

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Iterator[Product2[K, C]] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          keys.clear()
          combiners.clear()
          val firstPair = sorted.next()
          keys += firstPair._1
          combiners += firstPair._2
          val key = firstPair._1
          while (sorted.hasNext && comparator.compare(sorted.head._1, key) == 0) {
            val pair = sorted.next()
            var i = 0
            var foundKey = false
            while (i < keys.size && !foundKey) {
              if (keys(i) == pair._1) {
                combiners(i) = mergeCombiners(combiners(i), pair._2)
                foundKey = true
              }
              i += 1
            }
            if (!foundKey) {
              keys += pair._1
              combiners += pair._2
            }
          }

          // Note that we return an iterator of elements since we could've had many keys marked
          // equal by the partial order; we flatten this below to get a flat iterator of (K, C).
          keys.iterator.zip(combiners.iterator)
        }
      }.flatMap(i => i)
    } else {
      // We have a total ordering, so the objects with the same key are sequential.
      new Iterator[Product2[K, C]] {
        val sorted = mergeSort(iterators, comparator).buffered

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Product2[K, C] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          val elem = sorted.next()
          val k = elem._1
          var c = elem._2
          while (sorted.hasNext && sorted.head._1 == k) {
            val pair = sorted.next()
            c = mergeCombiners(c, pair._2)
          }
          (k, c)
        }
      }
    }
  }

  class SpillReader(writeBuffer: PmemBlockObjectStream) {
    // Each spill reader is relate to one partition
    // which is different from spark original codes (relate to one spill file)
    val blockId = writeBuffer.getBlockId()
    var indexInBatch: Int = 0
    var partitionMetaIndex: Int = 0

    var inStream: InputStream = _
    var inObjStream: DeserializationStream = _
    var nextItem: (K, C) = _
    var total_records: Long = 0

    loadStream()

    def readPartitionIter(partitionId: Int): Iterator[Product2[K, C]] =
      new Iterator[Product2[K, C]] {
      override def hasNext: Boolean = {
        if (nextItem == null) {
          nextItem = readNextItem()
          if (nextItem == null) {
            return false
          }
        }
        return dep.partitioner.getPartition(nextItem._1) == partitionId
      }

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val item = nextItem
        nextItem = null
        item
      }
    }

    private def readNextItem(): (K, C) = {
      if (inObjStream == null) {
        if (inStream != null) {
          inStream.close()
          inStream.asInstanceOf[PmemInputStream].deleteBlock()
        }
        return null
      }
      val k = inObjStream.readObject().asInstanceOf[K]
      val c = inObjStream.readObject().asInstanceOf[C]
      indexInBatch += 1
      if (indexInBatch == total_records) {
        inObjStream = null
      }
      (k, c)
    }

    def loadStream(): Unit = {
      total_records = writeBuffer.getTotalRecords()
      inStream = writeBuffer.getInputStream()
      val wrappedStream = serializerManager.wrapStream(blockId, inStream)
      inObjStream = serInstance.deserializeStream(wrappedStream)
      indexInBatch = 0
    }
  }
}
