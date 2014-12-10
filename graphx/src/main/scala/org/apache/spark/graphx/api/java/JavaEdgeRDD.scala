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

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.language.implicitConversions

import scala.reflect.ClassTag

class JavaEdgeRDD[ED: ClassTag, VD: ClassTag]
  (edges: RDD[Edge[ED]])
  extends JavaEdgeRDDLike[ED, VD, JavaEdgeRDD[ED, VD], JavaRDD[Edge[ED]]] {

  override def wrapRDD(edgeRDD: RDD[Edge[ED]]): JavaRDD[Edge[ED]] = JavaRDD.fromRDD(edgeRDD)

  override def edgeRDD: EdgeRDD[ED, VD] = EdgeRDD.fromEdges(edges)

  /** Persist RDDs of this EdgeRDD with the default storage level (MEMORY_ONLY_SER) */
  def cache(): JavaEdgeRDD[ED, VD] = edges.cache().asInstanceOf[JavaEdgeRDD[ED, VD]]

  /** Persist RDDs of this EdgeRDD with the default storage level (MEMORY_ONLY_SER) */
  def persist(): JavaEdgeRDD[ED, VD] = edges.persist().asInstanceOf[JavaEdgeRDD[ED, VD]]

  /** Persist the RDDs of this DStream with the given storage level */
  def persist(storageLevel: StorageLevel): JavaEdgeRDD[ED, VD] =
    edges.persist(storageLevel).asInstanceOf[JavaEdgeRDD[ED, VD]]

  def unpersist(blocking: Boolean = true) : JavaEdgeRDD[ED, VD] =
    JavaEdgeRDD.fromEdgeRDD(edgeRDD.unpersist(blocking))
}

object JavaEdgeRDD {

  implicit def fromEdgeRDD[ED: ClassTag, VD: ClassTag]
    (edges: EdgeRDD[ED, VD]): JavaEdgeRDD[ED, VD] =
      new JavaEdgeRDD(edges)
}

