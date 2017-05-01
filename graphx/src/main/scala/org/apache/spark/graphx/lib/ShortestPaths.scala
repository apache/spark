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

package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import org.apache.spark.graphx._

/**
* Computes shortest paths to the given set of landmark vertices, returning a graph where each
* vertex attribute is a map containing the shortest-path distance to each reachable landmark.
*/
object ShortestPaths {
  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type SPMap = Map[VertexId, Int]

 /**
  * Stores a pair which contains the shortest dist to source vertex
  * and a List contains all the paths to source
  */
  type SSSPPair = (Double, List[Seq[VertexId]])

  private def makeMap(x: (VertexId, Int)*) = Map(x: _*)

  private def incrementMap(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d + 1) }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

 /**
  * Computes shortest paths to the given set of landmark vertices.
  *
  * @tparam ED the edge attribute type (not used in the computation)
  *
  * @param graph the graph for which to compute the shortest paths
  * @param landmarks the list of landmark vertex ids. Shortest paths will be computed to each
  * landmark.
  *
  * @return a graph where each vertex attribute is a map containing the shortest-path distance to
  * each reachable landmark vertex.
  */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], landmarks: Seq[VertexId]): Graph[SPMap, ED] = {
    val spGraph = graph.mapVertices { (vid, attr) =>
      if (landmarks.contains(vid)) makeMap(vid -> 0) else makeMap()
    }

    val initialMessage = makeMap()

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge.dstAttr)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }

    Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
  }

 /**
  * Computes shortest paths from the source vertex to all other vertices.
  * Weights of edges in the graph should be positive
  *
  * @tparam VD the vertex attribute type
  *
  * @param graph the graph for which to compute the shortest paths
  * @param sourceId source vertex
  * @param weightedGraph weighted or unweighted graph
  *
  * @return a graph where each vertex attribute is a pair
  *         containing the shortest-path distance from the source to this vertex
  *         and all shortest-paths from the source to this vertex
  */
  def run[VD: ClassTag](graph: Graph[VD, Double],
                                sourceId : VertexId,
                                weightedGraph: Boolean)
  : Graph[SSSPPair, Double] = {

    // guarantee the source vertex exists in the graph
    val sourceCount = graph.vertices.filter(v => v._1 == sourceId).count()
    require(sourceCount == 1, "invalid source Id, this id doesn't exist")

    // guarantee all edges be positive
    val negativeEdgeCount = graph.edges.filter(edge => edge.attr <= 0).count()
    require(negativeEdgeCount == 0, "edge weight should be greater than 0")

    // initialize the graph:
    // if the vertex is source, initialize it with the 0.0 as the shortest distance,
    // a list with the sourceId as the shortest path;
    // else, initialize the vertex with a maximum double as the shortest distance,
    // an empty list as the shortest path.
    val initialGraph : Graph[(Double, List[Seq[VertexId]]), Double] = {
      val temp = graph.mapVertices {
        (id, _) => {
          if (id == sourceId) (0.0, List(Seq[VertexId](sourceId)))
          else (Double.PositiveInfinity, List[Seq[VertexId]]())
        }
      }
      // unweighted graph, simply change the edge attribute to 1.0
      if(!weightedGraph) {
        temp.mapEdges(_ => 1.0)
      } else {
        temp
      }
    }

    // initial massage: set shortest path distance to infinity and initialize a empty list
    val initialMsg: SSSPPair = (Double.PositiveInfinity, List[Seq[VertexId]]())

    // set up maximum iteration times
    val maxIterations: Int = Int.MaxValue

    // set up active direction as 'out'
    val activeDirection: EdgeDirection = EdgeDirection.Out

    // Vertex Program that receiving messages
    // runs on each vertex and receives the inbound message and
    // computes a new vertex value.
    // On the first iteration the vertex program is invoked on
    // all vertices and is passed the default message.
    // On subsequent iterations the vertex program is only invoked
    // on those vertices that receive messages.
    // 'distAndPaths' is the type of SSSPPair,
    // so distAndPath._1 is the shortest distance the current node holds from source,
    // distAndPath._2 is the shortest path the current node holds from source,
    // if a new msg with shorter distance comes, update the vertex with the msg
    // else if the msg holds the same shortest dist as the current node,
    // appending the shortest paths of the msg to the shortest paths of the current node,
    // and then remove the duplicate path.
    def vprog(id: VertexId, distAndPaths: SSSPPair, msg: SSSPPair): SSSPPair = {
      if (distAndPaths._1 < msg._1) distAndPaths
      else if (distAndPaths._1 == msg._1) {
        (distAndPaths._1, (distAndPaths._2 ::: msg._2).distinct)
      }
      else msg
    }

    // Send Message
    // a user supplied function that is applied to out edges of vertices
    // that received messages in the current iteration,
    // determine the messages to send out for the next iteration and where to send it.
    // if a vertex receive a message in the current iteration
    // the 'sendMsg' function will be applied to it,
    // and try to find a shorter or equal length path,
    // send the updated msg with the corresponding dist and the path list
    def sendMsg(triplet: EdgeTriplet[SSSPPair, Double]): Iterator[(VertexId, SSSPPair)] = {
      if (triplet.srcAttr._1 + triplet.attr <= triplet.dstAttr._1) {
        Iterator{
          (triplet.dstId,
            (triplet.srcAttr._1 + triplet.attr,
              triplet.srcAttr._2.map(curPaths => curPaths :+ triplet.dstId)))
        }
      } else {
        Iterator.empty
      }
    }

    // Merge Message
    // user defined function to merge multiple messages arriving at the same vertex
    // at the start of a Superstep before applying the vertex program 'vprog'
    // get two messages of type - SSSPPair, and compare them,
    // get the one which holds the shorter distance
    // if two messages come in with the equal shortest dist, combine the two msg into one
    // e.g. a vertex get 2 messages(msg1 and msg2) with different dist,
    // mergeMsg will compare dist in msg1 and msg2
    // and merge them into one msg which contains the shorter distance
    def mergeMsg(msg1: SSSPPair, msg2: SSSPPair): SSSPPair = {
      if (msg1._1 < msg2._1) msg1
      else if (msg1._1 == msg2._1) {
        (msg1._1, (msg1._2 ::: msg2._2).distinct)
      }
      else msg2
    }

    initialGraph.pregel(initialMsg, maxIterations, activeDirection)(vprog, sendMsg, mergeMsg)
  }
}

