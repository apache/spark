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
import scala.collection.mutable.{ArrayBuffer, PriorityQueue}

/**
 * A wrapper for SpillableAppendOnlyMap that handles two cases:
 *
 * (1)  If a mergeCombiners function is specified, merge values into combiners before disk
 *      spill, as it is possible to merge the resulting combiners later.
 *
 * (2)  Otherwise, group values of the same key together before disk spill, and merge them
 *      into combiners only after reading them back from disk.
 */
class ExternalAppendOnlyMap[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C)
  extends Iterable[(K, C)] with Serializable {

  private val mergeBeforeSpill: Boolean = mergeCombiners != null

  private val map: SpillableAppendOnlyMap[K, V, _, C] = {
    if (mergeBeforeSpill) {
      new SpillableAppendOnlyMap[K, V, C, C] (createCombiner, mergeValue,
        mergeCombiners, Predef.identity)
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
        mergeGroups, combineGroup)
    }
  }

  def insert(key: K, value: V): Unit = map.insert(key, value)

  override def iterator: Iterator[(K, C)] = map.iterator
}

/**
 * An append-only map that spills sorted content to disk when the memory threshold is exceeded.
 * A group is an intermediate combiner, with type M equal to either C or ArrayBuffer[V].
 */
class SpillableAppendOnlyMap[K, V, M, C](
    createGroup: V => M,
    mergeValue: (M, V) => M,
    mergeGroups: (M, M) => M,
    createCombiner: M => C)
  extends Iterable[(K, C)] with Serializable {

  var currentMap = new SizeTrackingAppendOnlyMap[K, M]
  val oldMaps = new ArrayBuffer[DiskIterator]
  val memoryThreshold = {
    val bufferSize = System.getProperty("spark.shuffle.buffer", "1024").toLong * 1024 * 1024
    val bufferPercent = System.getProperty("spark.shuffle.buffer.percent", "0.8").toFloat
    bufferSize * bufferPercent
  }

  def insert(key: K, value: V): Unit = {
    val update: (Boolean, M) => M = (hadVal, oldVal) => {
      if (hadVal) mergeValue(oldVal, value) else createGroup(value)
    }
    currentMap.changeValue(key, update)
    if (currentMap.estimateSize() > memoryThreshold) {
      spill()
    }
  }

  def spill(): Unit = {
    val file = File.createTempFile("external_append_only_map", "")
    val out = new ObjectOutputStream(new FileOutputStream(file))
    val sortedMap = currentMap.iterator.toList.sortBy(kv => kv._1.hashCode())
    sortedMap.foreach(out.writeObject)
    out.close()
    currentMap = new SizeTrackingAppendOnlyMap[K, M]
    oldMaps.append(new DiskIterator(file))
  }

  override def iterator: Iterator[(K, C)] = new ExternalIterator()

  // An iterator that sort-merges (K, M) pairs from memory and disk into (K, C) pairs
  class ExternalIterator extends Iterator[(K, C)] {

    // Order by key hash value
    val pq = PriorityQueue[KMITuple]()(Ordering.by(_.key.hashCode()))
    val inputStreams = Seq(new MemoryIterator(currentMap)) ++ oldMaps
    inputStreams.foreach(readFromIterator)

    // Read from the given iterator until a key of different hash is retrieved
    def readFromIterator(it: Iterator[(K, M)]): Unit = {
      var minHash : Option[Int] = None
      while (it.hasNext) {
        val (k, m) = it.next()
        pq.enqueue(KMITuple(k, m, it))
        minHash match {
          case None => minHash = Some(k.hashCode())
          case Some(expectedHash) if k.hashCode() != expectedHash => return
        }
      }
    }

    override def hasNext: Boolean = !pq.isEmpty

    override def next(): (K, C) = {
      val minKMI = pq.dequeue()
      var (minKey, minGroup) = (minKMI.key, minKMI.group)
      val minHash = minKey.hashCode()
      readFromIterator(minKMI.iterator)

      // Merge groups with the same key into minGroup
      var collidedKMI = ArrayBuffer[KMITuple]()
      while (!pq.isEmpty && pq.head.key.hashCode() == minHash) {
        val newKMI = pq.dequeue()
        if (newKMI.key == minKey) {
          minGroup = mergeGroups(minGroup, newKMI.group)
          readFromIterator(newKMI.iterator)
        } else {
          // Collision
          collidedKMI += newKMI
        }
      }
      collidedKMI.foreach(pq.enqueue(_))
      (minKey, createCombiner(minGroup))
    }

    case class KMITuple(key: K, group: M, iterator: Iterator[(K, M)])
  }

  // Iterate through (K, M) pairs in sorted order from the in-memory map
  class MemoryIterator(map: AppendOnlyMap[K, M]) extends Iterator[(K, M)] {
    val sortedMap = currentMap.iterator.toList.sortBy(km => km._1.hashCode())
    val it = sortedMap.iterator
    override def hasNext: Boolean = it.hasNext
    override def next(): (K, M) = it.next()
  }

  // Iterate through (K, M) pairs in sorted order from an on-disk map
  class DiskIterator(file: File) extends Iterator[(K, M)] {
    val in = new ObjectInputStream(new FileInputStream(file))
    var nextItem: Option[(K, M)] = None

    override def hasNext: Boolean = {
      nextItem = try {
        Some(in.readObject().asInstanceOf[(K, M)])
      } catch {
        case e: EOFException => None
      }
      nextItem.isDefined
    }

    override def next(): (K, M) = {
      nextItem match {
        case Some(item) => item
        case None => throw new NoSuchElementException
      }
    }
  }
}
