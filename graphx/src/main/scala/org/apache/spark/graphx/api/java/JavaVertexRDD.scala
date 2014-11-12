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
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.graphx.{VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, TaskContext}

import scala.language.implicitConversions
import scala.reflect._

/**
 * A Java-friendly interface to [[org.apache.spark.graphx.VertexRDD]], the vertex
 * RDD abstraction in Spark GraphX that represents a vertex class in a graph.
 * Vertices can be created from existing RDDs or it can be generated from transforming
 * existing VertexRDDs using operations such as `mapValues`, `pagerank`, etc.
 * For operations applicable to vertices in a graph in GraphX, please refer to
 * [[org.apache.spark.graphx.VertexRDD]]
 */

class JavaVertexRDD[VD](
    val vertices: RDD[(VertexId, VD)],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    (implicit val classTag: ClassTag[VD])
  extends JavaVertexRDDLike[VD, JavaVertexRDD[VD], JavaRDD[(VertexId, VD)]] {

  override def vertexRDD = VertexRDD(vertices)

  override def wrapRDD(in: RDD[(VertexId, VD)]): JavaRDD[(VertexId, VD)] = {
    JavaRDD.fromRDD(in)
  }

  /** Persist RDDs of this DStream with the default storage level (MEMORY_ONLY_SER) */
  def cache(): JavaVertexRDD[VD] = vertices.cache().asInstanceOf[JavaVertexRDD[VD]]

  /** Persist RDDs of this DStream with the default storage level (MEMORY_ONLY_SER) */
  def persist(): JavaVertexRDD[VD] = vertices.persist().asInstanceOf[JavaVertexRDD[VD]]

  /** Persist the RDDs of this DStream with the given storage level */
  def persist(storageLevel: StorageLevel): JavaVertexRDD[VD] =
    vertices.persist(storageLevel).asInstanceOf[JavaVertexRDD[VD]]

  def unpersist(blocking: Boolean = true) : this.type =
    JavaVertexRDD(vertices.unpersist(blocking))

  override def compute(part: Partition, context: TaskContext): Iterator[(VertexId, VD)] =
    vertexRDD.compute(part, context)


  def asJavaVertexRDD = JavaRDD.fromRDD(this.vertexRDD)






}

object JavaVertexRDD {

  implicit def fromVertexRDD[VD: ClassTag](vertices: JavaRDD[(VertexId, VD)]): JavaVertexRDD[VD] =
    new JavaVertexRDD[VD](vertices)

  implicit def apply[VD: ClassTag](vertices: JavaRDD[(Long, VD)]): JavaVertexRDD[VD] = {
    new JavaVertexRDD[VD](vertices)
  }
}


