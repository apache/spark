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

import java.lang.{Double => JDouble, Long => JLong}

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

class JavaGraph[@specialized VD: ClassTag, @specialized ED: ClassTag]
  (vertexRDD : VertexRDD[VD], edgeRDD: EdgeRDD[ED]) {

  def vertices: JavaVertexRDD[VD] = JavaVertexRDD(vertexRDD)
  def edges: JavaEdgeRDD[ED] = JavaEdgeRDD(edgeRDD)
  @transient lazy val graph : Graph[VD, ED] = Graph(vertexRDD, edgeRDD)

  def partitionBy(partitionStrategy: PartitionStrategy, numPartitions: Int): JavaGraph[VD, ED] = {
    val graph = Graph(vertexRDD, edgeRDD)
    JavaGraph(graph.partitionBy(partitionStrategy, numPartitions))
  }

  /** The number of edges in the graph. */
  def numEdges: JLong = edges.count()

  /** The number of vertices in the graph. */
  def numVertices: JLong = vertices.count()

  def inDegrees: JavaVertexRDD[Int] = JavaVertexRDD[Int](graph.inDegrees)

  def outDegrees: JavaVertexRDD[Int] = JavaVertexRDD[Int](graph.outDegrees)

  def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2) : JavaGraph[VD2, ED] = {
    JavaGraph(graph.mapVertices(map))
  }

  def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): JavaGraph[VD, ED2] = {
    JavaGraph(graph.mapEdges(map))
  }

  def mapTriplets[ED2: ClassTag](map: EdgeTriplet[VD, ED] => ED2): JavaGraph[VD, ED2] = {
    JavaGraph(graph.mapTriplets(map))
  }

  def reverse : JavaGraph[VD, ED] = JavaGraph(graph.reverse)

  def subgraph(
    epred: EdgeTriplet[VD,ED] => Boolean = (x => true),
    vpred: (VertexId, VD) => Boolean = ((v, d) => true)) : JavaGraph[VD, ED] = {
    JavaGraph(graph.subgraph(epred, vpred))
  }

  def groupEdges(merge: (ED, ED) => ED): JavaGraph[VD, ED] = {
    JavaGraph(graph.groupEdges(merge))
  }

  @deprecated("use aggregateMessages", "1.2.0")
  def mapReduceTriplets[A: ClassTag](
    mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
    reduceFunc: (A, A) => A,
    activeSetOpt: Option[(VertexRDD[_], EdgeDirection)] = None) : JavaVertexRDD[A] = {
    JavaVertexRDD(graph.mapReduceTriplets(mapFunc, reduceFunc, activeSetOpt))
  }

  def aggregateMessages[A: ClassTag](
    sendMsg: EdgeContext[VD, ED, A] => Unit,
    mergeMsg: (A, A) => A,
    tripletFields: TripletFields = TripletFields.All) : JavaVertexRDD[A] = {
    JavaVertexRDD(graph.aggregateMessages(sendMsg, mergeMsg, tripletFields))
  }

  def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
    (mapFunc: (VertexId, VD, Option[U]) => VD2) : JavaGraph[VD2, ED] = {
    JavaGraph(graph.outerJoinVertices(other)(mapFunc))
  }

  def pagerank(tol: Double, resetProb: Double = 0.15) : JavaGraph[Double, Double] =
    JavaGraph(PageRank.runUntilConvergence(graph, tol, resetProb))
}

object JavaGraph {

//  implicit def apply[VD: ClassTag, ED: ClassTag]
//    (vertexRDD: RDD[(VertexId, VD)], edges: RDD[Edge[ED]]): JavaGraph[VD, ED] = {
//    new JavaGraph[VD, ED](VertexRDD(vertexRDD), EdgeRDD.fromEdges(edges))
//  }

  implicit def apply[VD: ClassTag, ED: ClassTag]
    (graph: Graph[VD, ED]): JavaGraph[VD, ED] = {
    new JavaGraph[VD, ED](graph.vertices, EdgeRDD.fromEdges[ED, VD](graph.edges))
  }

  implicit def apply [VD: ClassTag, ED: ClassTag]
    (vertices: JavaVertexRDD[VD], edges: JavaEdgeRDD[ED]): JavaGraph[VD, ED] = {
    new JavaGraph(VertexRDD(vertices.toRDD), edges.edgeRDD)
  }
}

