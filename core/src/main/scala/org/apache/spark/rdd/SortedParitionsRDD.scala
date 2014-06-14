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

package org.apache.spark.rdd

import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer
import java.io.{InputStream, BufferedInputStream, FileInputStream, File, Serializable, EOFException}
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, BlockManager}
import org.apache.spark.util.SizeEstimator

private[spark] class SortedPartitionsRDD[T: ClassTag](
    prev: RDD[T],
    lt: (T, T) => Boolean)
  extends RDD[T](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override val partitioner = prev.partitioner    // Since sorting partitions cannot change a partition's keys

  override def compute(split: Partition, context: TaskContext) = {
    new SortedIterator(firstParent[T].iterator(split, context), lt)
  }
}

private[spark] class SortedIterator[T](iter: Iterator[T], lt: (T, T) => Boolean) extends Iterator[T] with Logging {
  private val sparkConf = SparkEnv.get.conf
  // Collective memory threshold shared across all running tasks
  private val maxMemoryThreshold = {
    val memoryFraction = sparkConf.getDouble("spark.shuffle.memoryFraction", 0.3)
    val safetyFraction = sparkConf.getDouble("spark.shuffle.safetyFraction", 0.8)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }

  private val sorted = doSort()
  
  def hasNext : Boolean = {
    sorted.hasNext
  }
  
  def next : T = {
    sorted.next
  }
  
  private def doSort() : Iterator[T] = {
    val subLists = new ArrayBuffer[Iterator[T]]()

    // keep the first sub list in memory
    subLists += nextSubList

    while (iter.hasNext) {
      // spill remaining sub lists to disk
      var diskBuffer = new DiskBuffer[T]()
      diskBuffer ++= nextSubList
      subLists += diskBuffer.iterator
    }
    println("Merge sorting one in-memory list with %d external list(s)".format(subLists.size - 1))
    logInfo("Merge sorting one in-memory list with %d external list(s)".format(subLists.size - 1))

    merge(subLists)
  }

  private def nextSubList() : Iterator[T] = {
    var subList = new SizeTrackingArrayBuffer[T](10)
    while (fitsInMemory(subList) && iter.hasNext) {
      subList += iter.next
    }
    return subList.sortWith(lt).iterator
  }
  
  private def fitsInMemory(list : SizeTrackingArrayBuffer[T]) : Boolean = {
    // TODO: use maxMemoryThreshold
    list.estimateSize < 10000
  }

  private def merge(list : ArrayBuffer[Iterator[T]]) : Iterator[T] = {
    if (list.size == 1) {
      return list(0)
    }
    if (list.size == 2) {
      return doMerge(list(0), list(1))
    }
    val mid = list.size >> 1
    val left = merge(list.slice(0, mid))
    val right = merge(list.slice(mid, list.size))
    doMerge(left, right)
  }

  private def doMerge(it1 : Iterator[T], it2 : Iterator[T]) : Iterator[T] = {
    var array = new DiskBuffer[T]()
// for testing...
//    var array = new ArrayBuffer[T]()
    if (!it1.hasNext) {
      array ++= it2
      return array.iterator
    }
    if (!it2.hasNext) {
      array ++= it1
      return array.iterator
    }
    var t1 = it1.next
    var t2 = it2.next
    while (true) {
      if (lt(t1, t2)) {
        array += t1
        if (it1.hasNext) {
          t1 = it1.next
        } else {
          array += t2
          array ++= it2
          return array.iterator
        }
      } else {
        array += t2
        if (it2.hasNext) {
          t2 = it2.next
        } else {
          array += t1
          array ++= it1
          return array.iterator
        }
      }
    }
    array.iterator
  }
}

private class SizeTrackingArrayBuffer[T](initialSize : Int) {
  private var array = new ArrayBuffer[T](initialSize)
  private var averageSize : Double = 0.0
  private var nextSampleNum : Int = 1

  def +=(elem: T): this.type = {
    array += elem
    updateAverage
    this
  }

  def ++=(xs: TraversableOnce[T]): this.type = {
    array ++= xs
    updateAverage
    this
  }

  def size : Int = {
    array.size
  }

  def sortWith(lt: (T, T) => Boolean): this.type = {
    array = array.sortWith(lt)
    this
  }

  def iterator : Iterator[T] = {
    array.iterator
  }

  def updateAverage = {
    if (array.size >= nextSampleNum) {
      averageSize = SizeEstimator.estimate(array)
      averageSize /= array.size
      nextSampleNum <<= 1
      assert(nextSampleNum < 0x40000000)
    }
  }

  def estimateSize(): Long = {
    (array.size * averageSize).toLong
  }
}

private class DiskBuffer[T] {
  private val serializer = SparkEnv.get.serializer
  private val blockManager = SparkEnv.get.blockManager
  private val diskBlockManager = blockManager.diskBlockManager
  private val sparkConf = SparkEnv.get.conf
  private val fileBufferSize = sparkConf.getInt("spark.shuffle.file.buffer.kb", 100) * 1024

  val (blockId, file) = diskBlockManager.createTempBlock()
  var writer = blockManager.getDiskWriter(blockId, file, serializer, fileBufferSize)
  var numObjects : Int = 0

  def +=(elem: T): this.type = {
    numObjects += 1
    writer.write(elem)
    this
  }

  def ++=(xs: TraversableOnce[T]): this.type = {
    xs.foreach({ numObjects += 1; writer.write(_) })
    this
  }

  def iterator : Iterator[T] = {
    writer.close
    val fileBufferSize = sparkConf.getInt("spark.shuffle.file.buffer.kb", 100) * 1024
    new DiskBufferIterator(file, blockId, serializer, fileBufferSize)
  }
}

private class DiskBufferIterator[T](file: File, blockId: BlockId, serializer: Serializer, fileBufferSize : Int) extends Iterator[T] {
  private val fileStream = new FileInputStream(file)
  private val bufferedStream = new BufferedInputStream(fileStream, fileBufferSize)
  private var compressedStream = SparkEnv.get.blockManager.wrapForCompression(blockId, bufferedStream)
  private var deserializeStream = serializer.newInstance.deserializeStream(compressedStream)
  private var nextItem = None : Option[T]
  
  def hasNext : Boolean = {
    nextItem match {
      case Some(item) => true
      case None => nextItem = doNext()
    }
    nextItem match {
      case Some(item) => true
      case None => false
    }
  }
  
  def next() : T = {
    nextItem match {
      case Some(item) =>
        nextItem = None
        item
      case None =>
        doNext match {
          case Some(item) => item
          case None => throw new NoSuchElementException
        }
    }
  }

  private def doNext() : Option[T] = {
    try {
      Some(deserializeStream.readObject().asInstanceOf[T])
    } catch {
      case e: EOFException =>
        cleanup
        None
    }
  }

  private def cleanup() = {
    deserializeStream.close()
    file.delete()
  }
}
