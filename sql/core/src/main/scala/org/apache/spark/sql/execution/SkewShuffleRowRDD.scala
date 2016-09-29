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

package org.apache.spark.sql.execution

import java.util.Arrays

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow

class SkewCoalescedPartitioner(
        val parent: Partitioner,
        val partitionStartIndices: Array[(Int, Int)])
  extends Partitioner {

  @transient private lazy val parentPartitionMapping: Array[Int] = {
    val n = parent.numPartitions
    val result = new Array[Int](n)
    for (i <- 0 until partitionStartIndices.length) {
      val start = partitionStartIndices(i)._2
      val end = if (i < partitionStartIndices.length - 1) partitionStartIndices(i + 1)._2 else n
      for (j <- start until end) {
        result(j) = i
      }
    }
    result
  }

  override def numPartitions: Int = partitionStartIndices.length

  override def getPartition(key: Any): Int = {
    parentPartitionMapping(parent.getPartition(key))
  }

  override def equals(other: Any): Boolean = other match {
    case c: SkewCoalescedPartitioner =>
      c.parent == parent &&
        c.partitionStartIndices.zip(partitionStartIndices).
          forall( r => r match {
            case (x, y) => (x._1 == y._1 && x._2 == y._2)
            })
    case _ =>
      false
  }

  override def hashCode(): Int = 31 * parent.hashCode() + partitionStartIndices.hashCode()
}

 /**
  * if mapIndex is -1, same as ShuffledRowRDDPartition
  * if mapIndex > -1 ,only read one block of mappers.
  */
private final class SkewShuffledRowRDDPartition(
    val postShufflePartitionIndex: Int,
    val mapIndex: Int,
    val startPreShufflePartitionIndex: Int,
    val endPreShufflePartitionIndex: Int) extends Partition {
  override val index: Int = postShufflePartitionIndex

  override def hashCode(): Int = postShufflePartitionIndex

  override def equals(other: Any): Boolean = super.equals(other)
}

 /**
  * only use for skew data join. In join case , need fetch the same partition of
  * left output and rigth output together. but when some partiton have bigger data than
  * other partitions, it occur data skew . in the case , we need a specialized RDD to handling this.
  * in skew partition side,we don't produce one partition, because one partition produce
  * one task deal so much data is too slaw . but produce per-stage mapping task num parititons.
  * one task only deal one mapper data. in other no skew side. In order to deal with the
  * corresponding skew partition , we need produce same partition per-stage parititon num
  * times.(Equivalent to broadcoast this partition)
  *
  * other no skew partition, then deal like ShuffledRowRDD
  */
class SkewShuffleRowRDD(
    var dependency1: ShuffleDependency[Int, InternalRow, InternalRow],
    partitionStartIndices: Array[(Int, Int, Int)])
  extends ShuffledRowRDD ( dependency1, None) {

  private[this] val numPreShufflePartitions = dependency.partitioner.numPartitions

  override def getPartitions: Array[Partition] = {
    val partitions = ArrayBuffer[Partition]()
    var partitionIndex = -1
    for(i <- 0 until partitionStartIndices.length ) {
      partitionStartIndices(i) match {
        case (isSkew, partition, prePartitionNum) =>
          if (isSkew > 0) {
            (0 until prePartitionNum).
              foreach(x => {
                partitionIndex += 1
                val part: Partition = if (isSkew == 1) {
                  new SkewShuffledRowRDDPartition(partitionIndex, x, partition, partition + 1)
                } else {
                  new SkewShuffledRowRDDPartition(partitionIndex, -1, partition, partition + 1)
                }
                partitions += part
              })
          } else {
            partitionIndex += 1
            val endIdx = if (i < partitionStartIndices.length - 1) {
              partitionStartIndices(i + 1)._2
            } else {
              numPreShufflePartitions
            }
            partitions +=
              new SkewShuffledRowRDDPartition(partitionIndex, -1, partition, endIdx)
          }
      }
    }
    partitions.toArray
  }
  // Todo: get mapidx location
  override def getPreferredLocations(partition: Partition): Seq[String] = Nil

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val skewShuffledRowRDDPartition = split.asInstanceOf[SkewShuffledRowRDDPartition]
    val reader =
      SparkEnv.get.shuffleManager.getReader(
        dependency.shuffleHandle,
        skewShuffledRowRDDPartition.startPreShufflePartitionIndex,
        skewShuffledRowRDDPartition.endPreShufflePartitionIndex,
        context,
        skewShuffledRowRDDPartition.mapIndex)
    reader.read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)
  }

}
