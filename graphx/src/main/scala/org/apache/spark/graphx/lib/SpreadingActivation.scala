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

import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Implementation of Spreading Activation algorithm.
 *
 * Spreading Activation is an algorithm that allows to spread out an activation from a starting
 * node. The activation will be distributed to the neighbors. The formula for calculating the
 * activation of the neighbors can be passed as parameter.
 *
 * This implementation allows to start the spreading activation from multiple nodes. The result
 * will be provided separate. This allows to either sum the values later or the run two spreading
 * activations simultaneously and to use clusters more homogeneous.
 *
 *
 * @see <a href="http://www.websci11.org/fileadmin/websci/posters/105_paper.pdf">Spreading
 *      Activation for Web Scale Reasoning</a>.
 */
object SpreadingActivation extends Logging {

  /**
   * Run Spreading Activation for multiple starting nodes with a fixed number of iterations and/or
   * a threshold. The result is calculated for each starting node separate.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (is provided as parameter for the `activationFunc`
   *
   * @param g the graph on which to compute the Spreading Activation
   * @param startNodes a list of [[VertexId]] that represents the starting nodes
   * @param maxIteration the maximum number of iterations of Spreading Activation
   * @param threshold a threshold that it used to filter out impulses that just has a minimal impact
   * @param edgeDirection the direction of the impulses. Is not allowed to be
   *                      [[EdgeDirection.Either]]
   * @param activationFunc a function that calculated the activation for a neighbour. The old power
   *                       of the impulse, the degree of the vertex and the edge attributes are
   *                       given as input parameters
   * @return a [[RDD]] that exists of a tuple with a vertex id and a map. The vertex id represents
   *         the id in the graph. The map provides the result for this node for every starting node.
   */
  def run[VD: ClassTag, ED: ClassTag](
                                       g: Graph[VD, ED],
                                       startNodes: List[VertexId],
                                       maxIteration: Int = Integer.MAX_VALUE,
                                       threshold: Double = 0.000005,
                                       edgeDirection: EdgeDirection = EdgeDirection.Out,
                                       activationFunc: (Double, Int, ED) => Double
                                       = (old: Double, degree: Int, attr: ED) => old / degree)
  : RDD[(VertexId, Map[VertexId, Double])] = {

    def getIterator(edgeDirection: EdgeDirection,
                    edge: EdgeTriplet[
                      (Map[VertexId, Double], Int, Map[VertexId, List[Impulse]]), ED])
    : Iterator[(VertexId, Map[VertexId, List[Impulse]])] = {
      edgeDirection match {
        case EdgeDirection.Out =>
          if (edge.srcAttr._3.forall(_._2.isEmpty)) {
            Iterator.empty
          } else {
            // calculate the power that should send with each impulse. If the power of an impulse
            // is less than the threshold, it is filtered out
            val impulsesToSend = edge.srcAttr._3.map((t) => {
              val newList = t._2.map((i) => {
                new Impulse(activationFunc(i.power, edge.srcAttr._2, edge.attr),
                  i.history ++ List[VertexId](edge.srcId))
              }).filter(_.power >= threshold)
              (t._1, newList)
            })
            Iterator((edge.dstId, impulsesToSend))
          }
        case EdgeDirection.In =>
          if (edge.dstAttr._3.forall(_._2.isEmpty)) {
            Iterator.empty
          } else {
            // calculate the power that should send with each impulse. If the power of an impulse
            // is less than the threshold, it is filtered out
            val impulsesToSend = edge.dstAttr._3.map((t) => {
              val newList = t._2.map((i) => {
                new Impulse(activationFunc(i.power, edge.dstAttr._2, edge.attr),
                  i.history ++ List[VertexId](edge.dstId))
              }).filter(_.power >= threshold)
              (t._1, newList)
            })
            Iterator((edge.srcId, impulsesToSend))
          }
      }
    }

    def sendImpulses(edge: EdgeTriplet[
      (Map[VertexId, Double], Int, Map[VertexId, List[Impulse]]), ED])
    : Iterator[(VertexId, Map[VertexId, List[Impulse]])] = {
      edgeDirection match {
        case EdgeDirection.Out =>
          getIterator(EdgeDirection.Out, edge)
        case EdgeDirection.In =>
          getIterator(EdgeDirection.In, edge)
        case EdgeDirection.Either =>
          // If either is chosen, the reversed edges has been added to the graph. So either
          // out or in could be used here
          getIterator(EdgeDirection.Out, edge)
      }
    }

    def combineMsg(a: Map[VertexId, List[Impulse]], b: Map[VertexId, List[Impulse]])
    : Map[VertexId, List[Impulse]] = {
      a ++ b map {
        case (id, list) => id -> (list ++ a.getOrElse(id, List[Impulse]()))
      }
    }

    def vertexProgram(vid: VertexId,
                      v: (Map[VertexId, Double], Int, Map[VertexId, List[Impulse]]),
                      m: Map[VertexId, List[Impulse]])
    : (Map[VertexId, Double], Int, Map[VertexId, List[Impulse]]) = {

      // Check in first iteration if this vertex (vid) is one of the starting vertices. If not,
      // filter out the impulses
      val message = m.map((t) => {
        val filteredImpulses = t._2 filter {
          case m: StartImpulse if m.start == vid => true
          case m: StartImpulse => false
          case _ => true
        }
        (t._1, filteredImpulses)
      })

      // Filter out impulses that hit this vertex already
      val nonCycledImpulses = message.map((t) => {
        val filteredImpulses = t._2 filter(!_.history.contains(vid))
        (t._1, filteredImpulses)
      })

      val activationToAdd = nonCycledImpulses.map((t) => (t._1, t._2.map(_.power).sum))

      // Merge both maps and sum values
      val newActivation = v._1 ++ activationToAdd.map({
        case (id, value) => id -> (value + v._1.getOrElse(id, 0.0))
      })


      // All non cycled impulses will be sent. The vid is added later to the history to reduce
      // the number of created objects
      (newActivation, v._2, nonCycledImpulses)
    }

    // if edge direction is 'either' extend edges by reversed edges.
    val preparedGraph = {
      edgeDirection match {
        case EdgeDirection.Either =>
          Graph(g.vertices, g.edges ++ g.edges.reverse)
        case EdgeDirection.Both =>
          throw new IllegalArgumentException("edge direction should not be both")
        case _ => g
      }
    }

    // select the degree depending on the edge direction.
    val degreesToJoin = edgeDirection match {
      case EdgeDirection.In => preparedGraph.inDegrees
      case EdgeDirection.Out => preparedGraph.outDegrees
      case EdgeDirection.Either => preparedGraph.outDegrees
    }

    // Build graph for spreading activation. The triple for each vertex contains the current
    // activation value (Double), the degree, depending on the edge direction (Int), and a list
    // of messages that were received in that iteration
    val graph: Graph[(Map[VertexId, Double], Int, Map[VertexId, List[Impulse]]), ED] = preparedGraph
      .outerJoinVertices(degreesToJoin) {
      (_, _, deg) => (Map[VertexId, Double](), deg.getOrElse(0), Map[VertexId, List[Impulse]]())
    }

    val initMsg: Map[VertexId, List[Impulse]] =
      startNodes.map((id) => (id, List[Impulse](new StartImpulse(id)))).toMap

    val ed = edgeDirection match {
      case EdgeDirection.Either => EdgeDirection.Out
      case _ => edgeDirection
    }

    val result =
      Pregel[(Map[VertexId, Double], Int, Map[VertexId, List[Impulse]]), ED,
        Map[VertexId, List[Impulse]]](graph, initMsg, maxIteration, ed)(vertexProgram,
          sendImpulses, combineMsg)

    result.vertices.map((t) => (t._1, t._2._1))
  }

  private class Impulse(val power: Double, val history: List[VertexId] = List())
    extends Serializable

  private class StartImpulse(val start: VertexId) extends Impulse(1)
}
