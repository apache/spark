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

import java.lang.{Long => JLong}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.{EdgePartition, EdgeRDDImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * EdgeRDD['ED', 'VD'] is a column-oriented edge partition RDD created from RDD[Edge[ED]].
 * JavaEdgeRDD class provides a Java API to access implementations of the EdgeRDD class
 *
 * @param partitionsRDD
 * @param targetStorageLevel
 * @tparam ED
 * @tparam VD
 */
class JavaEdgeRDD[ED: ClassTag, VD: ClassTag]
  (val partitionsRDD: RDD[(PartitionID, EdgePartition[ED, VD])],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends JavaEdgeRDDLike[ED, VD, JavaEdgeRDD[ED, VD],
    JavaRDD[(PartitionID, EdgePartition[ED, VD])]] {

  /* Convert RDD[(PartitionID, EdgePartition[ED, VD])] to EdgeRDD[ED, VD] */
  override def edgeRDD: EdgeRDDImpl[ED, VD] = {
    new EdgeRDDImpl(partitionsRDD, targetStorageLevel)
  }

  /**
   * Java Wrapper for RDD of Edges
   *
   * @param edgeRDD
   * @return
   */
  override def wrapRDD(edgeRDD: RDD[(PartitionID, EdgePartition[ED, VD])]) :
    JavaRDD[(PartitionID, EdgePartition[ED, VD])] = {
    JavaRDD.fromRDD(edgeRDD)
  }

  /** Persist RDDs of this JavaEdgeRDD with the default storage level (MEMORY_ONLY_SER) */
  def cache(): this.type = {
    partitionsRDD.persist(StorageLevel.MEMORY_ONLY)
    this
  }

  /** Persist the RDDs of this JavaEdgeRDD with the given storage level */
  def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  def unpersist(blocking: Boolean = true) : this.type = {
    edgeRDD.unpersist(blocking)
    this
  }

  override def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): JavaEdgeRDD[ED2, VD] = {
    edgeRDD.mapValues(f)
  }

  override def reverse: JavaEdgeRDD[ED, VD] = edgeRDD.reverse

  override def filter
    (epred: EdgeTriplet[VD, ED] => Boolean,
    vpred: (VertexId, VD) => Boolean): JavaEdgeRDD[ED, VD] = {
    edgeRDD.filter(epred, vpred)
  }

  override def innerJoin[ED2: ClassTag, ED3: ClassTag]
    (other: EdgeRDD[ED2])
    (f: (VertexId, VertexId, ED, ED2) => ED3): JavaEdgeRDD[ED3, VD] = {
    edgeRDD.innerJoin(other)(f)
  }

  override def mapEdgePartitions[ED2: ClassTag, VD2: ClassTag]
  (f: (PartitionID, EdgePartition[ED, VD]) => EdgePartition[ED2, VD2]): JavaEdgeRDD[ED2, VD2] = {
    edgeRDD.mapEdgePartitions(f)
  }
}

object JavaEdgeRDD {

  implicit def apply[ED: ClassTag, VD: ClassTag]
    (edges: EdgeRDDImpl[ED, VD]): JavaEdgeRDD[ED, VD] = {
    new JavaEdgeRDD(edges.partitionsRDD)
  }

  implicit def apply[ED: ClassTag, VD: ClassTag](edges: JavaRDD[Edge[ED]]) : JavaEdgeRDD[ED, VD] = {
    JavaEdgeRDD(EdgeRDD.fromEdges[ED, VD](edges.rdd))
  }

  def toEdgeRDD[ED: ClassTag, VD: ClassTag](edges: JavaEdgeRDD[ED, VD]): EdgeRDDImpl[ED, VD] = {
    edges.edgeRDD
  }

  def fromRDDOfEdges[ED: ClassTag, VD: ClassTag](edges: RDD[Edge[ED]]) : JavaEdgeRDD[ED, VD] = {
    JavaEdgeRDD[ED, VD](EdgeRDD.fromEdges[ED, VD](edges))
  }
}

