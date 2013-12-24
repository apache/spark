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
 * A simple map that spills sorted content to disk when the memory threshold is exceeded. A combiner
 * function must be specified to merge values back into memory during read.
 */
class ExternalAppendOnlyMap[K,V](combinerFunction: (V, V) => V,
                                 memoryThresholdMB: Int = 1024)
  extends Iterable[(K,V)] with Serializable {

  var currentMap = new AppendOnlyMap[K,V]
  var oldMaps = new ArrayBuffer[DiskKVIterator]

  def changeValue(key: K, updateFunc: (Boolean, V) => V): Unit = {
    currentMap.changeValue(key, updateFunc)
    val mapSize = SizeEstimator.estimate(currentMap)
    //if (mapSize > memoryThresholdMB * math.pow(1024, 2)) {
    if (mapSize > 1024 * 10) {
      spill()
    }
  }

  def spill(): Unit = {
    println("SPILL")
    val file = File.createTempFile("external_append_only_map", "")  // Add spill location
    val out = new ObjectOutputStream(new FileOutputStream(file))
    val sortedMap = currentMap.iterator.toList.sortBy(kv => kv._1.hashCode())
    sortedMap foreach {
      out.writeObject( _ )
    }
    out.close()
    currentMap = new AppendOnlyMap[K,V]
    oldMaps.append(new DiskKVIterator(file))
  }

  override def iterator: Iterator[(K,V)] = new ExternalIterator()

  /**
   *  An iterator that merges KV pairs from memory and disk in sorted order
   */
  class ExternalIterator extends Iterator[(K, V)] {

    // Order by increasing key hash value
    implicit object KVOrdering extends Ordering[KVITuple] {
      def compare(a:KVITuple, b:KVITuple) = -a.key.hashCode().compareTo(b.key.hashCode())
    }
    val pq = mutable.PriorityQueue[KVITuple]()
    val inputStreams = Seq(new MemoryKVIterator(currentMap)) ++ oldMaps
    inputStreams foreach { readFromIterator }

    override def hasNext: Boolean = !pq.isEmpty

    override def next(): (K,V) = {
      println("ExternalIterator.next - How many left? "+pq.length)
      val minKVI = pq.dequeue()
      var (minKey, minValue, minIter) = (minKVI.key, minKVI.value, minKVI.iter)
//      println("Min key = "+minKey)
      readFromIterator(minIter)
      while (!pq.isEmpty && pq.head.key == minKey) {
        val newKVI = pq.dequeue()
        val (newValue, newIter) = (newKVI.value, newKVI.iter)
//        println("\tfound new value to merge! "+newValue)
//        println("\tcombinerFunction("+minValue+" <====> "+newValue+")")
        minValue = combinerFunction(minValue, newValue)
//        println("\tCombine complete! New value = "+minValue)
        readFromIterator(newIter)
      }
      println("Returning minKey = "+minKey+", minValue = "+minValue)
      (minKey, minValue)
    }

    def readFromIterator(iter: Iterator[(K,V)]): Unit = {
      if (iter.hasNext) {
        val (k, v) = iter.next()
        pq.enqueue(KVITuple(k, v, iter))
      }
    }

    case class KVITuple(key:K, value:V, iter:Iterator[(K,V)])
  }

  class MemoryKVIterator(map: AppendOnlyMap[K,V]) extends Iterator[(K,V)] {
    val sortedMap = currentMap.iterator.toList.sortBy(kv => kv._1.hashCode())
    val it = sortedMap.iterator
    override def hasNext: Boolean = it.hasNext
    override def next(): (K,V) = it.next()
  }

  class DiskKVIterator(file: File) extends Iterator[(K,V)] {
    val in = new ObjectInputStream(new FileInputStream(file))
    var nextItem:(K,V) = _
    var eof = false

    override def hasNext: Boolean = {
      if (eof) {
        return false
      }
      try {
        nextItem = in.readObject().asInstanceOf[(K,V)]
      } catch {
        case e: EOFException =>
          eof = true
          return false
      }
      true
    }

    override def next(): (K,V) = {
      if (eof) {
        throw new NoSuchElementException
      }
      nextItem
    }
  }
}
