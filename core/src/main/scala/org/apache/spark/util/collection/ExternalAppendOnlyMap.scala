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
import java.util.Comparator

import scala.collection.mutable.{ArrayBuffer, PriorityQueue}
import scala.reflect.ClassTag

import org.apache.spark.SparkEnv
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{DiskBlockManager, DiskBlockObjectWriter}

/**
 * A wrapper for SpillableAppendOnlyMap that handles two cases:
 *
 * (1)  If a mergeCombiners function is specified, merge values into combiners before disk
 *      spill, as it is possible to merge the resulting combiners later.
 *
 * (2)  Otherwise, group values of the same key together before disk spill, and merge them
 *      into combiners only after reading them back from disk.
 *
 * In the latter case, values occupy much more space because they are not collapsed as soon
 * as they are inserted. This in turn leads to more disk spills, degrading performance.
 * For this reason, a mergeCombiners function should be specified if possible.
 */
private[spark] class ExternalAppendOnlyMap[K, V, C: ClassTag](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    serializer: Serializer = SparkEnv.get.serializerManager.default,
    diskBlockManager: DiskBlockManager = SparkEnv.get.blockManager.diskBlockManager)
  extends Iterable[(K, C)] with Serializable {

  private val mergeBeforeSpill: Boolean = mergeCombiners != null

  private val map: SpillableAppendOnlyMap[K, V, _, C] = {
    if (mergeBeforeSpill) {
      new SpillableAppendOnlyMap[K, V, C, C] (createCombiner, mergeValue, mergeCombiners,
        Predef.identity, serializer, diskBlockManager)
    } else {
      // Use ArrayBuffer[V] as the intermediate combiner
      val createGroup: (V => ArrayBuffer[V]) = value => ArrayBuffer[V](value)
      val mergeValueIntoGroup: (ArrayBuffer[V], V) => ArrayBuffer[V] = (group, value) => {
        group += value
      }
      val mergeGroups: (ArrayBuffer[V], ArrayBuffer[V]) => ArrayBuffer[V] = (group1, group2) => {
        group1 ++= group2
      }
      val combineGroup: (ArrayBuffer[V] => C) = group => {
        var combiner : Option[C] = None
        group.foreach { v =>
          combiner match {
            case None => combiner = Some(createCombiner(v))
            case Some(c) => combiner = Some(mergeValue(c, v))
          }
        }
        combiner.getOrElse(null.asInstanceOf[C])
      }
      new SpillableAppendOnlyMap[K, V, ArrayBuffer[V], C](createGroup, mergeValueIntoGroup,
        mergeGroups, combineGroup, serializer, diskBlockManager)
    }
  }

  def insert(key: K, value: V): Unit = map.insert(key, value)

  override def iterator: Iterator[(K, C)] = map.iterator
}

/**
 * An append-only map that spills sorted content to disk when the memory threshold is exceeded.
 * A group is an intermediate combiner, with type G equal to either C or ArrayBuffer[V].
 *
 * This map takes two passes over the data:
 *   (1) Values are merged into groups, which are spilled to disk as necessary.
 *   (2) Groups are read from disk and merged into combiners, which are returned.
 *
 * If we never spill to disk, we avoid the second pass provided that groups G are already
 * combiners C.
 *
 * Note that OOM is still possible with the SpillableAppendOnlyMap. This may occur if the
 * collective G values do not fit into memory, or if the size estimation is not sufficiently
 * accurate. To account for the latter, `spark.shuffle.buffer.fraction` specifies an additional
 * margin of safety, while `spark.shuffle.buffer.mb` specifies the raw memory threshold.
 */
private[spark] class SpillableAppendOnlyMap[K, V, G: ClassTag, C: ClassTag](
    createGroup: V => G,
    mergeValue: (G, V) => G,
    mergeGroups: (G, G) => G,
    createCombiner: G => C,
    serializer: Serializer,
    diskBlockManager: DiskBlockManager)
  extends Iterable[(K, C)] with Serializable {

  import SpillableAppendOnlyMap._

  private var currentMap = new SizeTrackingAppendOnlyMap[K, G]
  private val oldMaps = new ArrayBuffer[DiskKGIterator]
  private val memoryThreshold = {
    val bufferSize = System.getProperty("spark.shuffle.buffer.mb", "1024").toLong * 1024 * 1024
    val bufferPercent = System.getProperty("spark.shuffle.buffer.fraction", "0.8").toFloat
    bufferSize * bufferPercent
  }
  private val fileBufferSize =
    System.getProperty("spark.shuffle.file.buffer.kb", "100").toInt * 1024
  private val comparator = new KeyGroupComparator[K, G]
  private val ser = serializer.newInstance()

  def insert(key: K, value: V): Unit = {
    val update: (Boolean, G) => G = (hadVal, oldVal) => {
      if (hadVal) mergeValue(oldVal, value) else createGroup(value)
    }
    currentMap.changeValue(key, update)
    if (currentMap.estimateSize() > memoryThreshold) {
      spill()
    }
  }

  private def spill(): Unit = {
    val (blockId, file) = diskBlockManager.createIntermediateBlock
    val writer = new DiskBlockObjectWriter(blockId, file, serializer, fileBufferSize, identity)
    try {
      val it = currentMap.destructiveSortedIterator(comparator)
      while (it.hasNext) {
        val kv = it.next()
        writer.write(kv)
      }
      writer.commit()
    } finally {
      // Partial failures cannot be tolerated; do not revert partial writes
      writer.close()
    }
    currentMap = new SizeTrackingAppendOnlyMap[K, G]
    oldMaps.append(new DiskKGIterator(file))
  }

  override def iterator: Iterator[(K, C)] = {
    if (oldMaps.isEmpty && implicitly[ClassTag[G]] == implicitly[ClassTag[C]]) {
      currentMap.iterator.asInstanceOf[Iterator[(K, C)]]
    } else {
      new ExternalIterator()
    }
  }

  // An iterator that sort-merges (K, G) pairs from memory and disk into (K, C) pairs
  private class ExternalIterator extends Iterator[(K, C)] {
    val mergeHeap = new PriorityQueue[KGITuple]
    val inputStreams = oldMaps ++ Seq(currentMap.destructiveSortedIterator(comparator))

    // Invariant: size of mergeHeap == number of input streams
    inputStreams.foreach{ it =>
      val kgPairs = readFromIterator(it)
      mergeHeap.enqueue(KGITuple(it, kgPairs))
    }

    // Read from the given iterator until a key of different hash is retrieved.
    // The resulting ArrayBuffer includes this key, and is ordered by key hash.
    def readFromIterator(it: Iterator[(K, G)]): ArrayBuffer[(K, G)] = {
      val kgPairs = new ArrayBuffer[(K, G)]
      if (it.hasNext) {
        var kg = it.next()
        kgPairs += kg
        val minHash = kg._1.hashCode()
        while (it.hasNext && kg._1.hashCode() == minHash) {
          kg = it.next()
          kgPairs += kg
        }
      }
      kgPairs
    }

    // Drop and return all (K, G) pairs with K = the given key from the given KGITuple
    def dropKey(kgi: KGITuple, key: K): ArrayBuffer[(K, G)] = {
      val dropped = new ArrayBuffer[(K, G)]
      var i = 0
      while (i < kgi.pairs.length) {
        if (kgi.pairs(i)._1 == key) {
          dropped += kgi.pairs.remove(i)
        } else {
          i += 1
        }
      }
      dropped
    }

    // Merge all (K, G) pairs with K = the given key into baseGroup
    def mergeIntoGroup(key: K, baseGroup: G, kgPairs: ArrayBuffer[(K, G)]): G = {
      var mergedGroup = baseGroup
      kgPairs.foreach { case (k, g) =>
        if (k == key){
          mergedGroup = mergeGroups(mergedGroup, g)
        }
      }
      mergedGroup
    }

    override def hasNext: Boolean = {
      mergeHeap.foreach{ kgi =>
        if (!kgi.pairs.isEmpty) {
          return true
        }
      }
      false
    }

    override def next(): (K, C) = {
      val minKGI = mergeHeap.dequeue()
      val (minPairs, minHash) = (minKGI.pairs, minKGI.minHash)
      if (minPairs.length == 0) {
        // Should only happen when hasNext is false
        throw new NoSuchElementException
      }
      var (minKey, minGroup) = minPairs(0)
      assert(minKey.hashCode() == minHash)

      // Merge the rest of minPairs into minGroup
      val minPairsWithKey = dropKey(minKGI, minKey).tail
      minGroup = mergeIntoGroup(minKey, minGroup, minPairsWithKey)
      if (minPairs.length == 0) {
        minPairs ++= readFromIterator(minKGI.iterator)
      }

      // Do the same for all other KGITuples with the same minHash
      val tuplesToAddBack = ArrayBuffer[KGITuple](minKGI)
      while (!mergeHeap.isEmpty && mergeHeap.head.minHash == minHash) {
        val newKGI = mergeHeap.dequeue()
        val pairsWithKey = dropKey(newKGI, minKey)
        minGroup = mergeIntoGroup(minKey, minGroup, pairsWithKey)
        if (newKGI.pairs.length == 0) {
          newKGI.pairs ++= readFromIterator(newKGI.iterator)
        }
        tuplesToAddBack += newKGI
      }
      tuplesToAddBack.foreach(mergeHeap.enqueue(_))
      (minKey, createCombiner(minGroup))
    }

    case class KGITuple(iterator: Iterator[(K, G)], pairs: ArrayBuffer[(K, G)])
      extends Comparable[KGITuple] {

      // Invariant: pairs are ordered by key hash
      def minHash: Int = {
        if (pairs.length > 0){
          pairs(0)._1.hashCode()
        } else {
          Int.MaxValue
        }
      }

      override def compareTo(other: KGITuple): Int = {
        // mutable.PriorityQueue dequeues the max, not the min
        -minHash.compareTo(other.minHash)
      }
    }
  }

  // Iterate through (K, G) pairs in sorted order from an on-disk map
  private class DiskKGIterator(file: File) extends Iterator[(K, G)] {
    val in = ser.deserializeStream(new FileInputStream(file))
    var nextItem: Option[(K, G)] = None
    var eof = false

    def readNextItem(): Option[(K, G)] = {
      if (!eof) {
        try {
          return Some(in.readObject().asInstanceOf[(K, G)])
        } catch {
          case e: EOFException =>
            eof = true
            cleanup()
        }
      }
      None
    }

    override def hasNext: Boolean = {
      nextItem match {
        case Some(item) => true
        case None =>
          nextItem = readNextItem()
          nextItem.isDefined
      }
    }

    override def next(): (K, G) = {
      nextItem match {
        case Some(item) =>
          nextItem = None
          item
        case None =>
          val item = readNextItem()
          item.getOrElse(throw new NoSuchElementException)
      }
    }

    // TODO: Ensure this gets called even if the iterator isn't drained.
    def cleanup() {
      file.delete()
    }
  }
}

private[spark] object SpillableAppendOnlyMap {
  private class KeyGroupComparator[K, G] extends Comparator[(K, G)] {
    def compare(kg1: (K, G), kg2: (K, G)): Int = {
      kg1._1.hashCode().compareTo(kg2._1.hashCode())
    }
  }
}
