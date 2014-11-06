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
package org.apache.spark.graphx.api.java

import org.apache.spark.api.java.{JavaRDD, JavaRDDLike}
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect._

/**
 * A Java-friendly interface to [[org.apache.spark.graphx.VertexRDD]], the vertex
 * RDD abstraction in Spark GraphX that represents a vertex class in a graph.
 * Vertices can be created from existing RDDs or it can be generated from transforming
 * existing VertexRDDs using operations such as `mapValues`, `pagerank`, etc.
 * For operations applicable to vertices in a graph in GraphX, please refer to
 * [[org.apache.spark.graphx.VertexRDD]]
 */

class JavaVertexRDD[@specialized VD: ClassTag](
    val parent: RDD[(VertexId, VD)],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends JavaVertexRDDLike[(VertexId, VD), JavaVertexRDD[VD]] {

//  val rdd = new VertexRDD(parent, targetStorageLevel)

//  val wrapVertexRDD(rdd: RDD[(VertexId, VD)]): This
//
//  override def innerJoin[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
//                                           (f: (VertexId, VD, U) => VD2): JavaVertexRDD[VD2] = {
//    other match {
//      case other: JavaVertexRDD[_] =>
//        innerZipJoin(other)(f)
//      case _ =>
//        this.withPartitionsRDD(
//          partitionsRDD.zipPartitions(
//            other.copartitionWithVertices(this.partitioner.get), preservesPartitioning = true) {
//            (partIter, msgs) => partIter.map(_.innerJoin(msgs)(f))
//          }
//        )
//    }
//  }
//
//  override def diff(other: VertexRDD[VD]): JavaRDD[(VertexId, VD)] = {
//    JavaRDD.fromRDD(super[VertexRDD].diff(other))
//  }
//
//  override def mapValues[VD2: ClassTag](f: VD => VD2): JavaVertexRDD[VD2] =
//    this.mapVertexPartitions(_.map((vid, attr) => f(attr)))
//
//  override def mapVertexPartitions[VD2: ClassTag]
//    (f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2]): JavaVertexRDD[VD2] = {
//    val newPartitionsRDD = partitionsRDD.mapPartitions(_.map(f), preservesPartitioning = true)
//    this.withPartitionsRDD(newPartitionsRDD)
//  }
//
//  override def withPartitionsRDD[VD2: ClassTag]
//    (partitionsRDD: RDD[ShippableVertexPartition[VD2]]): JavaVertexRDD[VD2] = {
//    new JavaVertexRDD[VD2](partitionsRDD.firstParent, this.targetStorageLevel)
//  }
//
//  def copartitionWithVertices(partitioner: Partitioner): JavaRDD[(VertexId, VD)] = {
//
//    val rdd = new ShuffledRDD[VertexId, VD, VD](rdd, partitioner)
//
//    // Set a custom serializer if the data is of int or double type.
//    if (classTag[VD] == ClassTag.Int) {
//      rdd.setSerializer(new IntAggMsgSerializer)
//    } else if (classTag[VD] == ClassTag.Long) {
//      rdd.setSerializer(new LongAggMsgSerializer)
//    } else if (classTag[VD] == ClassTag.Double) {
//      rdd.setSerializer(new DoubleAggMsgSerializer)
//    }
//    rdd
//  }
//
//  def innerZipJoin[U: ClassTag, VD2: ClassTag]
//    (other: JavaVertexRDD[U])
//    (f: (VertexId, VD, U) => VD2): JavaVertexRDD[VD2] = {
//    val newPartitionsRDD = partitionsRDD.zipPartitions(
//      other.partitionsRDD, preservesPartitioning = true
//    ) { (thisIter, otherIter) =>
//      val thisPart = thisIter.next()
//      val otherPart = otherIter.next()
//      Iterator(thisPart.innerJoin(otherPart)(f))
//    }
//    this.withPartitionsRDD(newPartitionsRDD)
//  }
  override def wrapRDD(rdd: RDD[(VertexId, VD)]): JavaRDD[(VertexId, VD)] = ???

  override def rdd: RDD[(VertexId, VD)] = ???

  override implicit val classTag: ClassTag[(VertexId, VD)] = _
}


