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
import scala.reflect.ClassTag

import org.apache.spark.SparkEnv
import org.apache.spark.serializer.Serializer

/**
 * A wrapper for SpillableAppendOnlyMap that handles two cases:
 *
 * (1)  If a mergeCombiners function is specified, merge values into combiners before disk
 *      spill, as it is possible to merge the resulting combiners later.
 *
 * (2)  Otherwise, group values of the same key together before disk spill, and merge them
 *      into combiners only after reading them back from disk.
 */
private[spark] class ExternalAppendOnlyMap[K, V, C: ClassTag](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C)
  extends Iterable[(K, C)] with Serializable {

  private val serializer = SparkEnv.get.serializerManager.default
  private val mergeBeforeSpill: Boolean = mergeCombiners != null

  private val map: SpillableAppendOnlyMap[K, V, _, C] = {
    if (mergeBeforeSpill) {
      new SpillableAppendOnlyMap[K, V, C, C] (createCombiner, mergeValue, mergeCombiners,
        Predef.identity, serializer)
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
        mergeGroups, combineGroup, serializer)
    }
  }

  def insert(key: K, value: V): Unit = map.insert(key, value)

  override def iterator: Iterator[(K, C)] = map.iterator
}

/**
 * An append-only map that spills sorted content to disk when the memory threshold is exceeded.
 * A group is an intermediate combiner, with type M equal to either C or ArrayBuffer[V].
 */
private[spark] class SpillableAppendOnlyMap[K, V, M: ClassTag, C: ClassTag](
    createGroup: V => M,
    mergeValue: (M, V) => M,
    mergeGroups: (M, M) => M,
    createCombiner: M => C,
    serializer: Serializer)
  extends Iterable[(K, C)] with Serializable {

  private var currentMap = new SizeTrackingAppendOnlyMap[K, M]
  private val oldMaps = new ArrayBuffer[DiskIterator]
  private val memoryThreshold = {
    val bufferSize = System.getProperty("spark.shuffle.buffer", "1024").toLong * 1024 * 1024
    val bufferPercent = System.getProperty("spark.shuffle.buffer.percent", "0.8").toFloat
    bufferSize * bufferPercent
  }
  private val ordering = new SpillableAppendOnlyMap.KeyHashOrdering[K, M]()
  private val ser = serializer.newInstance()

  def insert(key: K, value: V): Unit = {
    val update: (Boolean, M) => M = (hadVal, oldVal) => {
      if (hadVal) mergeValue(oldVal, value) else createGroup(value)
    }
    currentMap.changeValue(key, update)
    if (currentMap.estimateSize() > memoryThreshold) {
      spill()
    }
  }

  private def spill(): Unit = {
    val file = File.createTempFile("external_append_only_map", "")
    val out = ser.serializeStream(new FileOutputStream(file))
    val it = currentMap.destructiveSortedIterator(ordering)
    while (it.hasNext) {
      val kv = it.next()
      out.writeObject(kv)
    }
    out.close()
    currentMap = new SizeTrackingAppendOnlyMap[K, M]
    oldMaps.append(new DiskIterator(file))
  }

  override def iterator: Iterator[(K, C)] = {
    if (oldMaps.isEmpty && implicitly[ClassTag[M]] == implicitly[ClassTag[C]]) {
      currentMap.iterator.asInstanceOf[Iterator[(K, C)]]
    } else {
      new ExternalIterator()
    }
  }

  // An iterator that sort-merges (K, M) pairs from memory and disk into (K, C) pairs
  private class ExternalIterator extends Iterator[(K, C)] {
    val pq = new PriorityQueue[KMITuple]
    val inputStreams = Seq(currentMap.destructiveSortedIterator(ordering)) ++ oldMaps
    inputStreams.foreach(readFromIterator)

    // Read from the given iterator until a key of different hash is retrieved
    def readFromIterator(it: Iterator[(K, M)]): Unit = {
      var minHash : Option[Int] = None
      while (it.hasNext) {
        val (k, m) = it.next()
        pq.enqueue(KMITuple(k, m, it))
        minHash match {
          case None => minHash = Some(k.hashCode())
          case Some(expectedHash) =>
            if (k.hashCode() != expectedHash) {
              return
            }
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

    case class KMITuple(key: K, group: M, iterator: Iterator[(K, M)]) extends Ordered[KMITuple] {
      def compare(other: KMITuple): Int = {
        other.key.hashCode().compareTo(key.hashCode())
      }
    }
  }

  // Iterate through (K, M) pairs in sorted order from an on-disk map
  private class DiskIterator(file: File) extends Iterator[(K, M)] {
    val in = ser.deserializeStream(new FileInputStream(file))
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

private[spark] object SpillableAppendOnlyMap {
  private class KeyHashOrdering[K, M] extends Ordering[(K, M)] {
    def compare(x: (K, M), y: (K, M)): Int = {
      x._1.hashCode().compareTo(y._1.hashCode())
    }
  }
}
