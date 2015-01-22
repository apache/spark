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

import java.util.{List => JList}

import org.apache.spark.api.java.JavaRDDLike
import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2, Function3 => JFunction3}
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.{EdgeRDDImpl, ShippableVertexPartition}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, Partition, TaskContext}

import scala.language.implicitConversions
import scala.reflect.ClassTag

trait JavaVertexRDDLike[VD, This <: JavaVertexRDDLike[VD, This, R],
  R <: JavaRDDLike[(VertexId, VD), R]]
  extends Serializable with Logging {

  implicit val classTag: ClassTag[VD]

  // The type of the RDD is (VertexId, VD)
  def vertexRDD: VertexRDD[VD]

  def wrapRDD(in: RDD[(VertexId, VD)]): R

  override def toString: String = vertexRDD.toDebugString

  /**
   * Return an array of the first num values
   *
   * @param num
   * @return
   */
  def take(num: Int) : Array[(VertexId, VD)] = vertexRDD.take(num)

  def setName(name: String) = vertexRDD.setName(name)

  def compute(part: Partition, context: TaskContext): Iterator[(VertexId, VD)] = {
    vertexRDD.compute(part, context)
  }

  /**
   * To construct a new Java interface of VertexRDD that is indexed by only the visible vertices.
   * The resulting vertex RDD will be based on a different index and can no longer be quickly
   * joined with this RDD.
   */
  def reindex() : JavaVertexRDD[VD] = JavaVertexRDD(vertexRDD.reindex())

  /**
   * Applies a function to each `VertexPartition` of this RDD and returns a new
   * [[org.apache.spark.graphx.api.java.JavaVertexRDD]]
   */
  def mapVertexPartitions[VD2: ClassTag](
    f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2]) : JavaVertexRDD[VD2] = {
    JavaVertexRDD(vertexRDD.mapVertexPartitions(f))
  }

  def mapValues[VD2: ClassTag](f: VD => VD2): JavaVertexRDD[VD2] = {
    JavaVertexRDD(vertexRDD.mapValues(f))
  }

  /** Hides vertices that are the same between `this` and `other`; for vertices that are different,
    * keeps the values from `other`.
    */
  def diff(other: VertexRDD[VD]): JavaVertexRDD[VD] = {
    JavaVertexRDD(vertexRDD.diff(other))
  }

  /** Takes a [[org.apache.spark.graphx.api.java.JavaVertexRDD]] instead of a
    * [[org.apache.spark.graphx.VertexRDD]] as argument.
    */
  def diff(other: JavaVertexRDD[VD]): JavaVertexRDD[VD] = {
    JavaVertexRDD(vertexRDD.diff(other.vertexRDD))
  }

  def leftZipJoin[VD2: ClassTag, VD3: ClassTag]
    (other: VertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3): JavaVertexRDD[VD3] = {
    JavaVertexRDD(vertexRDD.leftZipJoin[VD2, VD3](other)(f))
  }

  def leftJoin[VD2: ClassTag, VD3: ClassTag]
    (other: RDD[(VertexId, VD2)])
    (f: (VertexId, VD, Option[VD2]) => VD3)
    : JavaVertexRDD[VD3] = {
    JavaVertexRDD(vertexRDD.leftJoin(other)(f))
  }

  def innerZipJoin[U: ClassTag, VD2: ClassTag]
    (other: VertexRDD[U])
    (f: (VertexId, VD, U) => VD2): JavaVertexRDD[VD2] = {
    JavaVertexRDD(vertexRDD.innerZipJoin(other)(f))
  }

  def innerJoin[U: ClassTag, VD2: ClassTag]
    (other: RDD[(VertexId, U)])
    (f: (VertexId, VD, U) => VD2): JavaVertexRDD[VD2] = {
    JavaVertexRDD(vertexRDD.innerJoin(other)(f))
  }

  def aggregateUsingIndex[VD2: ClassTag]
    (messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2): JavaVertexRDD[VD2] = {
    JavaVertexRDD(vertexRDD.aggregateUsingIndex(messages, reduceFunc))
  }

  def fromEdges[ED: ClassTag, VD: ClassTag]
    (edges: EdgeRDDImpl[ED, VD], numPartitions: Int, defaultVal: VD): JavaVertexRDD[VD] = {
    JavaVertexRDD(VertexRDD.fromEdges[VD]
      (EdgeRDD.fromEdges[ED, VD](edges), numPartitions, defaultVal))
  }
}
