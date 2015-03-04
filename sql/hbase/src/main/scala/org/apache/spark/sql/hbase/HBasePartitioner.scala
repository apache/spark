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

package org.apache.spark.sql.hbase

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{CollectionsUtils, Utils}
import org.apache.spark.{Partitioner, SparkEnv}

object HBasePartitioner {
  implicit object HBaseRawOrdering extends Ordering[HBaseRawType] {
    def compare(a: HBaseRawType, b: HBaseRawType) = Bytes.compareTo(a, b)
  }
}

class HBasePartitioner (var splitKeys: Array[HBaseRawType]) extends Partitioner {
  import HBasePartitioner.HBaseRawOrdering

  type t = HBaseRawType

  lazy private val len = splitKeys.length

  // For presplit table splitKeys(0) = bytes[0], to remove it,
  // otherwise partiton 0 always be empty and
  // we will miss the last region's date when bulk load
  lazy private val realSplitKeys = if (splitKeys.isEmpty) splitKeys else splitKeys.tail

  def numPartitions = if (len == 0) 1 else len

  @transient private val binarySearch: ((Array[t], t) => Int) = CollectionsUtils.makeBinarySearch[t]

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[t]
    var partition = 0
    if (len <= 128 && len > 0) {
      // If we have less than 128 partitions naive search
      val ordering = implicitly[Ordering[t]]
      while (partition < realSplitKeys.length && ordering.gt(k, realSplitKeys(partition))) {
        partition += 1
      }
    } else { // todo(wf): we need test this branch
      // Determine which binary search method to use only once.
      partition = binarySearch(realSplitKeys, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition - 1
      }
      if (partition > realSplitKeys.length) {
        partition = realSplitKeys.length
      }
    }
    partition
  }

  override def equals(other: Any): Boolean = other match {
    case r: HBasePartitioner =>
      r.splitKeys.sameElements(splitKeys)
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < splitKeys.length) {
      result = prime * result + splitKeys(i).hashCode
      i += 1
    }
    result = prime * result
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream) {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(splitKeys)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream) {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          splitKeys = ds.readObject[Array[HBaseRawType]]()
        }
    }
  }
}
