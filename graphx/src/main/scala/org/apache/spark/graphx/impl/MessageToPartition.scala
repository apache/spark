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

package org.apache.spark.graphx.impl

import scala.language.implicitConversions
import scala.reflect.{classTag, ClassTag}

import org.apache.spark.Partitioner
import org.apache.spark.graphx.{PartitionID, VertexId}
import org.apache.spark.rdd.{ShuffledRDD, RDD}


private[graphx]
class VertexBroadcastMsg[@specialized(Int, Long, Double, Boolean) T](
    @transient var partition: PartitionID,
    var vid: VertexId,
    var data: T)
  extends Product2[PartitionID, (VertexId, T)] with Serializable {

  override def _1 = partition

  override def _2 = (vid, data)

  override def canEqual(that: Any): Boolean = that.isInstanceOf[VertexBroadcastMsg[_]]
}


/**
 * A message used to send a specific value to a partition.
 * @param partition index of the target partition.
 * @param data value to send
 */
private[graphx]
class MessageToPartition[@specialized(Int, Long, Double, Char, Boolean/* , AnyRef */) T](
    @transient var partition: PartitionID,
    var data: T)
  extends Product2[PartitionID, T] with Serializable {

  override def _1 = partition

  override def _2 = data

  override def canEqual(that: Any): Boolean = that.isInstanceOf[MessageToPartition[_]]
}


private[graphx]
class VertexBroadcastMsgRDDFunctions[T: ClassTag](self: RDD[VertexBroadcastMsg[T]]) {
  def partitionBy(partitioner: Partitioner): RDD[VertexBroadcastMsg[T]] = {
    val rdd = new ShuffledRDD[PartitionID, (VertexId, T), (VertexId, T), VertexBroadcastMsg[T]](
      self, partitioner)

    // Set a custom serializer if the data is of int or double type.
    if (classTag[T] == ClassTag.Int) {
      rdd.setSerializer(new IntVertexBroadcastMsgSerializer)
    } else if (classTag[T] == ClassTag.Long) {
      rdd.setSerializer(new LongVertexBroadcastMsgSerializer)
    } else if (classTag[T] == ClassTag.Double) {
      rdd.setSerializer(new DoubleVertexBroadcastMsgSerializer)
    }
    rdd
  }
}


private[graphx]
class MsgRDDFunctions[T: ClassTag](self: RDD[MessageToPartition[T]]) {

  /**
   * Return a copy of the RDD partitioned using the specified partitioner.
   */
  def partitionBy(partitioner: Partitioner): RDD[MessageToPartition[T]] = {
    new ShuffledRDD[PartitionID, T, T, MessageToPartition[T]](self, partitioner)
  }

}

private[graphx]
object MsgRDDFunctions {
  implicit def rdd2PartitionRDDFunctions[T: ClassTag](rdd: RDD[MessageToPartition[T]]) = {
    new MsgRDDFunctions(rdd)
  }

  implicit def rdd2vertexMessageRDDFunctions[T: ClassTag](rdd: RDD[VertexBroadcastMsg[T]]) = {
    new VertexBroadcastMsgRDDFunctions(rdd)
  }
}

private[graphx]
class VertexRDDFunctions[VD: ClassTag](self: RDD[(VertexId, VD)]) {
  def copartitionWithVertices(partitioner: Partitioner): RDD[(VertexId, VD)] = {
    val rdd = new ShuffledRDD[VertexId, VD, VD, (VertexId, VD)](self, partitioner)

    // Set a custom serializer if the data is of int or double type.
    if (classTag[VD] == ClassTag.Int) {
      rdd.setSerializer(new IntAggMsgSerializer)
    } else if (classTag[VD] == ClassTag.Long) {
      rdd.setSerializer(new LongAggMsgSerializer)
    } else if (classTag[VD] == ClassTag.Double) {
      rdd.setSerializer(new DoubleAggMsgSerializer)
    }
    rdd
  }
}

private[graphx]
object VertexRDDFunctions {
  implicit def rdd2VertexRDDFunctions[VD: ClassTag](rdd: RDD[(VertexId, VD)]) = {
    new VertexRDDFunctions(rdd)
  }
}
