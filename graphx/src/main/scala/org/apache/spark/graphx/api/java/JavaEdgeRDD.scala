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
import org.apache.spark.graphx.impl.EdgePartition
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * EdgeRDD['ED', 'VD'] is a column-oriented edge partition RDD created from RDD[Edge[ED]].
 * JavaEdgeRDD class provides a Java API to access implementations of the EdgeRDD class
 *
 * @param edges
 * @tparam ED
 * @tparam VD
 */
class JavaEdgeRDD[ED: ClassTag, VD: ClassTag]
  (edges: EdgeRDD[ED, VD])
  extends JavaEdgeRDDLike[ED, VD, JavaEdgeRDD[ED, VD], Edge[ED]] {

  /**
   * Java Wrapper for RDD of Edges
   *
   * @param edgeRDD
   * @return
   */
  def wrapRDD(edgeRDD: EdgeRDD[ED, VD]): JavaEdgeRDD[ED, VD] = new JavaEdgeRDD(edgeRDD)

  def edgeRDD = edges

  def count(): Long = edgeRDD.count()

  /** Persist RDDs of this EdgeRDD with the default storage level (MEMORY_ONLY_SER) */
  def cache(): JavaEdgeRDD[ED, VD] = edges.cache().asInstanceOf[JavaEdgeRDD[ED, VD]]

  /** Persist RDDs of this EdgeRDD with the default storage level (MEMORY_ONLY_SER) */
  def persist(): JavaEdgeRDD[ED, VD] = edges.persist().asInstanceOf[JavaEdgeRDD[ED, VD]]

  /** Persist the RDDs of this DStream with the given storage level */
  def persist(storageLevel: StorageLevel): JavaEdgeRDD[ED, VD] =
    edges.persist(storageLevel).asInstanceOf[JavaEdgeRDD[ED, VD]]

  def unpersist(blocking: Boolean = true) : JavaEdgeRDD[ED, VD] =
    JavaEdgeRDD(edgeRDD.unpersist(blocking))

  def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): JavaEdgeRDD[ED2, VD] = {
    JavaEdgeRDD[ED2, VD](edgeRDD.mapValues(f))
  }

  def reverse: JavaEdgeRDD[ED, VD] = edges.reverse.asInstanceOf[JavaEdgeRDD[ED, VD]]

  def filter
    (epred: EdgeTriplet[VD, ED] => Boolean,
    vpred: (VertexId, VD) => Boolean): JavaEdgeRDD[ED, VD] = {
    JavaEdgeRDD(edgeRDD.filter(epred, vpred))
  }

  def innerJoin[ED2: ClassTag, ED3: ClassTag]
    (other: EdgeRDD[ED2, _])
    (f: (VertexId, VertexId, ED, ED2) => ED3): JavaEdgeRDD[ED3, VD] = {
    JavaEdgeRDD(edgeRDD.innerJoin(other)(f))
  }

  def mapEdgePartitions[ED2: ClassTag, VD2: ClassTag]
  (f: (PartitionID, EdgePartition[ED, VD]) => EdgePartition[ED2, VD2]): JavaEdgeRDD[ED2, VD2] = {
    edges.mapEdgePartitions(f).asInstanceOf[JavaEdgeRDD[ED2, VD2]]
  }
}

object JavaEdgeRDD {

  implicit def apply[ED: ClassTag, VD: ClassTag]
    (edges: EdgeRDD[ED, VD]): JavaEdgeRDD[ED, VD] =
      new JavaEdgeRDD(edges)

  implicit def apply[ED: ClassTag, VD: ClassTag](edges: JavaRDD[Edge[ED]]) : JavaEdgeRDD[ED, VD] = {
    new JavaEdgeRDD[ED, VD](EdgeRDD.fromEdges(edges.rdd))
  }

  def toEdgeRDD[ED: ClassTag, VD: ClassTag](edges: JavaEdgeRDD[ED, VD]): EdgeRDD[ED, VD] = {
    edges.edgeRDD
  }

  def fromRDDOfEdges[ED: ClassTag, VD: ClassTag](edges: RDD[Edge[ED]]) : JavaEdgeRDD[ED, VD] = {
    new JavaEdgeRDD[ED, VD](EdgeRDD.fromEdges(edges))
  }
}

