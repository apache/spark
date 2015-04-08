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

import java.lang.{Integer => JInt, Long => JLong}

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.reflect.ClassTag

import com.google.common.base.Optional
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaPairRDD._
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.JavaUtils
import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2,
  Function3 => JFunction3, _}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

class JavaVertexRDD[VD](val vertexRDD: VertexRDD[VD])(implicit val vdTag: ClassTag[VD])
    extends JavaPairRDD[JLong, VD](vertexRDD.map(kv => (kv._1, kv._2))) {

  /**
   * Maps each vertex attribute, preserving the index.
   *
   * @tparam VD2 the type returned by the map function
   *
   * @param f the function applied to each value in the RDD
   * @return a new VertexRDD with values obtained by applying `f` to each of the entries in the
   * original VertexRDD
   */
  override def mapValues[VD2](f: JFunction[VD, VD2]): JavaVertexRDD[VD2] = {
    implicit val vd2Tag: ClassTag[VD2] = fakeClassTag
    vertexRDD.mapValues(f)
  }

  /**
   * Maps each vertex attribute, additionally supplying the vertex ID.
   *
   * @tparam VD2 the type returned by the map function
   *
   * @param f the function applied to each ID-value pair in the RDD
   * @return a new VertexRDD with values obtained by applying `f` to each of the entries in the
   * original VertexRDD.  The resulting VertexRDD retains the same index.
   */
  def mapValues[VD2](f: JFunction2[JLong, VD, VD2]): JavaVertexRDD[VD2] = {
    implicit val vd2Tag: ClassTag[VD2] = fakeClassTag
    vertexRDD.mapValues((id: VertexId, v) => f.call(id, v))
  }

  /**
   * Hides vertices that are the same between `this` and `other`; for vertices that are different,
   * keeps the values from `other`.
   */
  def diff(other: JavaVertexRDD[VD]): JavaVertexRDD[VD] =
    vertexRDD.diff(other)

  /**
   * Left joins this RDD with another VertexRDD with the same index. This function will fail if
   * both VertexRDDs do not share the same index. The resulting vertex set contains an entry for
   * each vertex in `this`.
   * If `other` is missing any vertex in this VertexRDD, `f` is passed `None`.
   *
   * @tparam VD2 the attribute type of the other VertexRDD
   * @tparam VD3 the attribute type of the resulting VertexRDD
   *
   * @param other the other VertexRDD with which to join.
   * @param f the function mapping a vertex id and its attributes in this and the other vertex set
   * to a new vertex attribute.
   * @return a VertexRDD containing the results of `f`
   */
  def leftZipJoin[VD2, VD3](
      other: JavaVertexRDD[VD2],
      f: JFunction3[JLong, VD, Optional[VD2], VD3]): JavaVertexRDD[VD3] = {
    implicit val vd2Tag: ClassTag[VD2] = fakeClassTag
    implicit val vd3Tag: ClassTag[VD3] = fakeClassTag
    vertexRDD.leftZipJoin(other) {
      (vid, a, bOpt) => f.call(vid, a, JavaUtils.optionToOptional(bOpt))
    }
  }

  /**
   * Left joins this JavaVertexRDD with an RDD containing vertex attribute pairs. If the other RDD
   * is backed by a JavaVertexRDD with the same index then the efficient [[leftZipJoin]]
   * implementation is used. The resulting JavaVertexRDD contains an entry for each vertex in
   * `this`. If `other` is missing any vertex in this JavaVertexRDD, `f` is passed `None`. If there
   * are duplicates, the vertex is picked arbitrarily.
   *
   * @tparam VD2 the attribute type of the other JavaVertexRDD
   * @tparam VD3 the attribute type of the resulting JavaVertexRDD
   *
   * @param other the other JavaVertexRDD with which to join
   * @param f the function mapping a vertex id and its attributes in this and the other vertex set
   * to a new vertex attribute.
   * @return a JavaVertexRDD containing all the vertices in this JavaVertexRDD with the attributes
   * emitted by `f`.
   */
  def leftJoin[VD2, VD3](
      other: JavaPairRDD[JLong, VD2],
      f: JFunction3[JLong, VD, Optional[VD2], VD3])
    : JavaVertexRDD[VD3] = {
    implicit val vd2Tag: ClassTag[VD2] = fakeClassTag
    implicit val vd3Tag: ClassTag[VD3] = fakeClassTag
    val scalaOther: RDD[(VertexId, VD2)] = other.rdd.map(kv => (kv._1, kv._2))
    vertexRDD.leftJoin(scalaOther) {
      (vid, a, bOpt) => f.call(vid, a, JavaUtils.optionToOptional(bOpt))
    }
  }

  /**
   * Efficiently inner joins this JavaVertexRDD with another JavaVertexRDD sharing the same index.
   * See [[innerJoin]] for the behavior of the join.
   */
  def innerZipJoin[U, VD2](
      other: JavaVertexRDD[U],
      f: JFunction3[JLong, VD, U, VD2]): JavaVertexRDD[VD2] = {
    implicit val uTag: ClassTag[U] = fakeClassTag
    implicit val vd2Tag: ClassTag[VD2] = fakeClassTag
    vertexRDD.innerZipJoin(other) { (id, a, b) => f.call(id, a, b) }
  }

  /**
   * Inner joins this VertexRDD with an RDD containing vertex attribute pairs. If the other RDD is
   * backed by a VertexRDD with the same index then the efficient [[innerZipJoin]] implementation
   * is used.
   *
   * @param other an RDD containing vertices to join. If there are multiple entries for the same
   * vertex, one is picked arbitrarily. Use [[aggregateUsingIndex]] to merge multiple entries.
   * @param f the join function applied to corresponding values of `this` and `other`
   * @return a VertexRDD co-indexed with `this`, containing only vertices that appear in both
   *         `this` and `other`, with values supplied by `f`
   */
  def innerJoin[U, VD2](
      other: JavaPairRDD[JLong, U],
      f: JFunction3[JLong, VD, U, VD2]): JavaVertexRDD[VD2] = {
    implicit val uTag: ClassTag[U] = fakeClassTag
    implicit val vd2Tag: ClassTag[VD2] = fakeClassTag
    val scalaOther: RDD[(VertexId, U)] = other.rdd.map(kv => (kv._1, kv._2))
    vertexRDD.innerJoin(scalaOther) { (id, a, b) => f.call(id, a, b) }
  }

  /**
   * Aggregates vertices in `messages` that have the same ids using `reduceFunc`, returning a
   * VertexRDD co-indexed with `this`.
   *
   * @param messages an RDD containing messages to aggregate, where each message is a pair of its
   * target vertex ID and the message data
   * @param reduceFunc the associative aggregation function for merging messages to the same vertex
   * @return a VertexRDD co-indexed with `this`, containing only vertices that received messages.
   * For those vertices, their values are the result of applying `reduceFunc` to all received
   * messages.
   */
  def aggregateUsingIndex[VD2](
      messages: JavaPairRDD[JLong, VD2],
      reduceFunc: JFunction2[VD2, VD2, VD2]): JavaVertexRDD[VD2] = {
    implicit val vd2Tag: ClassTag[VD2] = fakeClassTag
    val scalaMessages: RDD[(VertexId, VD2)] = messages.rdd.map(kv => (kv._1, kv._2))
    vertexRDD.aggregateUsingIndex(scalaMessages, reduceFunc)
  }

  /** Prepares this VertexRDD for efficient joins with the given EdgeRDD. */
  def withEdges(edges: JavaEdgeRDD[_, _]): JavaVertexRDD[VD] = vertexRDD.withEdges(edges)
}

object JavaVertexRDD {

  /**
   * Constructs a standalone `VertexRDD` (one that is not set up for efficient joins with an
   * [[EdgeRDD]]) from an RDD of vertex-attribute pairs. Duplicate entries are removed arbitrarily.
   *
   * @tparam VD the vertex attribute type
   *
   * @param vertices the collection of vertex-attribute pairs
   */
  def create[VD](vertices: JavaPairRDD[JLong, VD]): JavaVertexRDD[VD] = {
    implicit val vdTag: ClassTag[VD] = fakeClassTag
    val scalaVertices: RDD[(VertexId, VD)] = vertices.rdd.map(kv => (kv._1, kv._2))
    VertexRDD(scalaVertices)
  }

  /**
   * Constructs a `VertexRDD` from an RDD of vertex-attribute pairs. Duplicate vertex entries are
   * removed arbitrarily. The resulting `VertexRDD` will be joinable with `edges`, and any missing
   * vertices referred to by `edges` will be created with the attribute `defaultVal`.
   *
   * @tparam VD the vertex attribute type
   *
   * @param vertices the collection of vertex-attribute pairs
   * @param edges the [[EdgeRDD]] that these vertices may be joined with
   * @param defaultVal the vertex attribute to use when creating missing vertices
   */
  def create[VD](vertices: JavaPairRDD[JLong, VD], edges: JavaEdgeRDD[_, _], defaultVal: VD)
    : JavaVertexRDD[VD] = {
    implicit val vdTag: ClassTag[VD] = fakeClassTag
    val scalaVertices: RDD[(VertexId, VD)] = vertices.rdd.map(kv => (kv._1, kv._2))
    VertexRDD(scalaVertices, edges, defaultVal)
  }

  /**
   * Constructs a `VertexRDD` from an RDD of vertex-attribute pairs. Duplicate vertex entries are
   * merged using `mergeFunc`. The resulting `VertexRDD` will be joinable with `edges`, and any
   * missing vertices referred to by `edges` will be created with the attribute `defaultVal`.
   *
   * @tparam VD the vertex attribute type
   *
   * @param vertices the collection of vertex-attribute pairs
   * @param edges the [[EdgeRDD]] that these vertices may be joined with
   * @param defaultVal the vertex attribute to use when creating missing vertices
   * @param mergeFunc the commutative, associative duplicate vertex attribute merge function
   */
  def create[VD](
      vertices: JavaPairRDD[JLong, VD], edges: JavaEdgeRDD[_, _], defaultVal: VD,
      mergeFunc: JFunction2[VD, VD, VD]): JavaVertexRDD[VD] = {
    implicit val vdTag: ClassTag[VD] = fakeClassTag
    val scalaVertices: RDD[(VertexId, VD)] = vertices.rdd.map(kv => (kv._1, kv._2))
    VertexRDD(scalaVertices, edges, defaultVal, mergeFunc)
  }

  /**
   * Constructs a `VertexRDD` containing all vertices referred to in `edges`. The vertices will be
   * created with the attribute `defaultVal`. The resulting `VertexRDD` will be joinable with
   * `edges`.
   *
   * @tparam VD the vertex attribute type
   *
   * @param edges the [[EdgeRDD]] referring to the vertices to create
   * @param numPartitions the desired number of partitions for the resulting `VertexRDD`
   * @param defaultVal the vertex attribute to use when creating missing vertices
   */
  def fromEdges[VD](
      edges: JavaEdgeRDD[_, _], numPartitions: Int, defaultVal: VD): JavaVertexRDD[VD] = {
    implicit val vdTag: ClassTag[VD] = fakeClassTag
    VertexRDD.fromEdges(edges, numPartitions, defaultVal)
  }

  implicit def fromVertexRDD[VD: ClassTag](rdd: VertexRDD[VD]): JavaVertexRDD[VD] =
    new JavaVertexRDD[VD](rdd)

  implicit def toVertexRDD[VD](rdd: JavaVertexRDD[VD]): VertexRDD[VD] =
    rdd.vertexRDD
}
