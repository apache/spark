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

package org.apache.spark.util

import java.io._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

/**
 * An append-only map that spills sorted content to disk when the memory threshold is exceeded.
 */
class ExternalAppendOnlyMap[K, V, C] (createCombiner: V => C,
                                      mergeValue: (C, V) => C,
                                      mergeCombiners: (C, C) => C,
                                      memoryThresholdMB: Int = 1024)
  extends Iterable[(K, C)] with Serializable {

  var currentMap = new AppendOnlyMap[K, C]
  var oldMaps = new ArrayBuffer[DiskKCIterator]

  def insert(key: K, value: V): Unit = {
    currentMap.changeValue(key, updateFunction(value))
    val mapSize = SizeEstimator.estimate(currentMap)
    if (mapSize > memoryThresholdMB * math.pow(1024, 2)) {
      spill()
    }
  }

  def updateFunction(value: V) : (Boolean, C) => C = {
    (hadVal: Boolean, oldVal: C) =>
      if (hadVal) mergeValue(oldVal, value) else createCombiner(value)
  }

  def spill(): Unit = {
    val file = File.createTempFile("external_append_only_map", "")  // Add spill location
    val out = new ObjectOutputStream(new FileOutputStream(file))
    val sortedMap = currentMap.iterator.toList.sortBy(kv => kv._1.hashCode())
    sortedMap.foreach { out.writeObject( _ ) }
    out.close()
    currentMap = new AppendOnlyMap[K, C]
    oldMaps.append(new DiskKCIterator(file))
  }

  override def iterator: Iterator[(K, C)] = new ExternalIterator()

  /**
   *  An iterator that merges KV pairs from memory and disk in sorted order
   */
  class ExternalIterator extends Iterator[(K, C)] {

    // Order by increasing key hash value
    implicit object KVOrdering extends Ordering[KCITuple] {
      def compare(a:KCITuple, b:KCITuple) = -a.key.hashCode().compareTo(b.key.hashCode())
    }
    val pq = mutable.PriorityQueue[KCITuple]()
    val inputStreams = Seq(new MemoryKCIterator(currentMap)) ++ oldMaps
    inputStreams.foreach { readFromIterator( _ ) }

    override def hasNext: Boolean = !pq.isEmpty

    // Combine all values from all input streams corresponding to the same key
    override def next(): (K, C) = {
      val minKCI = pq.dequeue()
      var (minKey, minCombiner) = (minKCI.key, minKCI.combiner)
      val minHash = minKey.hashCode()
      readFromIterator(minKCI.iter)

      var collidedKCI = ArrayBuffer[KCITuple]()
      while (!pq.isEmpty && pq.head.key.hashCode() == minHash) {
        val newKCI: KCITuple = pq.dequeue()
        if (newKCI.key == minKey){
          minCombiner = mergeCombiners(minCombiner, newKCI.combiner)
          readFromIterator(newKCI.iter)
        } else {
          // Collision
          collidedKCI += newKCI
        }
      }
      collidedKCI.foreach { pq.enqueue( _ ) }
      (minKey, minCombiner)
    }

    // Read from the given iterator until a key of different hash is retrieved,
    // Add each KC pair read from this iterator to the heap
    def readFromIterator(iter: Iterator[(K, C)]): Unit = {
      var minHash : Option[Int] = None
      while (iter.hasNext) {
        val (k, c) = iter.next()
        pq.enqueue(KCITuple(k, c, iter))
        minHash match {
          case None => minHash = Some(k.hashCode())
          case Some(expectedHash) =>
            if (k.hashCode() != expectedHash){
              return
            }
        }
      }
    }

    case class KCITuple(key:K, combiner:C, iter:Iterator[(K, C)])
  }

  class MemoryKCIterator(map: AppendOnlyMap[K, C]) extends Iterator[(K, C)] {
    val sortedMap = currentMap.iterator.toList.sortBy(kc => kc._1.hashCode())
    val it = sortedMap.iterator
    override def hasNext: Boolean = it.hasNext
    override def next(): (K, C) = it.next()
  }

  class DiskKCIterator(file: File) extends Iterator[(K, C)] {
    val in = new ObjectInputStream(new FileInputStream(file))
    var nextItem:(K, C) = _
    var eof = false

    override def hasNext: Boolean = {
      if (eof) {
        return false
      }
      try {
        nextItem = in.readObject().asInstanceOf[(K, C)]
      } catch {
        case e: EOFException =>
          eof = true
          return false
      }
      true
    }

    override def next(): (K, C) = {
      if (eof) {
        throw new NoSuchElementException
      }
      nextItem
    }
  }
}
