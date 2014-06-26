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

package org.apache.spark.graphx

import scala.reflect.ClassTag
import org.apache.spark.Logging


/**
 * Implements a Pregel-like bulk-synchronous message-passing API.
 *
 * Unlike the original Pregel API, the GraphX Pregel API factors the sendMessage computation over
 * edges, enables the message sending computation to read both vertex attributes, and constrains
 * messages to the graph structure.  These changes allow for substantially more efficient
 * distributed execution while also exposing greater flexibility for graph-based computation.
 *
 * @example We can use the Pregel abstraction to implement PageRank:
 * {{{
 * val pagerankGraph: Graph[Double, Double] = graph
 *   // Associate the degree with each vertex
 *   .outerJoinVertices(graph.outDegrees) {
 *     (vid, vdata, deg) => deg.getOrElse(0)
 *   }
 *   // Set the weight on the edges based on the degree
 *   .mapTriplets(e => 1.0 / e.srcAttr)
 *   // Set the vertex attributes to the initial pagerank values
 *   .mapVertices((id, attr) => 1.0)
 *
 * def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double =
 *   resetProb + (1.0 - resetProb) * msgSum
 * def sendMessage(id: VertexId, edge: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] =
 *   Iterator((edge.dstId, edge.srcAttr * edge.attr))
 * def messageCombiner(a: Double, b: Double): Double = a + b
 * val initialMessage = 0.0
 * // Execute Pregel for a fixed number of iterations.
 * Pregel(pagerankGraph, initialMessage, numIter)(
 *   vertexProgram, sendMessage, messageCombiner)
 * }}}
 *
 */
object Pregel extends Logging {

  class Context(val iteration: Int)

  class VertexContext(iteration: Int, val wasActive: Boolean, var isActive: Boolean = true) extends Context(iteration) {
    def deactivate() { isActive = false }
  }

  class EdgeContext(iteration: Int, val srcIsActive: Boolean, val dstIsActive: Boolean) extends Context(iteration)



  /**
   * Execute a Pregel-like iterative vertex-parallel abstraction.  The
   * user-defined vertex-program `vprog` is executed in parallel on
   * each vertex receiving any inbound messages and computing a new
   * value for the vertex.  The `sendMsg` function is then invoked on
   * all out-edges and is used to compute an optional message to the
   * destination vertex. The `mergeMsg` function is a commutative
   * associative function used to combine messages destined to the
   * same vertex.
   *
   * On the first iteration all vertices receive the `initialMsg` and
   * on subsequent iterations if a vertex does not receive a message
   * then the vertex-program is not invoked.
   *
   * This function iterates until there are no remaining messages, or
   * for `maxIterations` iterations.
   *
   * @tparam VD the vertex data type
   * @tparam ED the edge data type
   * @tparam A the Pregel message type
   *
   * @param graph the input graph.
   *
   * @param initialMsg the message each vertex will receive at the on
   * the first iteration
   *
   * @param maxIterations the maximum number of iterations to run for
   *
   * @param activeDirection the direction of edges incident to a vertex that received a message in
   * the previous round on which to run `sendMsg`. For example, if this is `EdgeDirection.Out`, only
   * out-edges of vertices that received a message in the previous round will run. The default is
   * `EdgeDirection.Either`, which will run `sendMsg` on edges where either side received a message
   * in the previous round. If this is `EdgeDirection.Both`, `sendMsg` will only run on edges where
   * *both* vertices received a message.
   *
   * @param vprog the user-defined vertex program which runs on each
   * vertex and receives the inbound message and computes a new vertex
   * value.  On the first iteration the vertex program is invoked on
   * all vertices and is passed the default message.  On subsequent
   * iterations the vertex program is only invoked on those vertices
   * that receive messages.
   *
   * @param sendMsg a user supplied function that is applied to out
   * edges of vertices that received messages in the current
   * iteration
   *
   * @param mergeMsg a user supplied function that takes two incoming
   * messages of type A and merges them into a single message of type
   * A.  ''This function must be commutative and associative and
   * ideally the size of A should not increase.''
   *
   * @return the resulting graph at the end of the computation
   *
   */
  @deprecated ("Switching to Pregel.run.", "1.1")
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
     (graph: Graph[VD, ED],
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED] =
  {
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
    // compute the messages
    var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
    var activeMessages = messages.count()
    // Loop
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      // Receive the messages. Vertices that didn't get any messages do not appear in newVerts.
      val newVerts = g.vertices.innerJoin(messages)(vprog).cache()
      // Update the graph with the new vertices.
      prevG = g
      g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
      g.cache()

      val oldMessages = messages
      // Send new messages. Vertices that didn't get any messages don't appear in newVerts, so don't
      // get to send messages. We must cache messages so it can be materialized on the next line,
      // allowing us to uncache the previous iteration.
      messages = g.mapReduceTriplets(sendMsg, mergeMsg, Some((newVerts, activeDirection))).cache()
      // The call to count() materializes `messages`, `newVerts`, and the vertices of `g`. This
      // hides oldMessages (depended on by newVerts), newVerts (depended on by messages), and the
      // vertices of prevG (depended on by newVerts, oldMessages, and the vertices of g).
      activeMessages = messages.count()

      logInfo("Pregel finished iteration " + i)

      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking=false)
      newVerts.unpersist(blocking=false)
      prevG.unpersistVertices(blocking=false)
      prevG.edges.unpersist(blocking=false)
      // count the iteration
      i += 1
    }

    g
  } // end of apply

  /**
   * Execute a Pregel-like iterative vertex-parallel abstraction.  The
   * user-defined vertex-program `vprog` is executed in parallel on
   * each vertex receiving any inbound messages and computing a new
   * value for the vertex.  The `sendMsg` function is then invoked on
   * all out-edges and is used to compute an optional message to the
   * destination vertex. The `mergeMsg` function is a commutative
   * associative function used to combine messages destined to the
   * same vertex.
   *
   * On the first iteration all vertices receive the `initialMsg` and
   * on subsequent iterations if a vertex does not receive a message
   * then the vertex-program is not invoked.
   *
   * This function iterates until there are no remaining messages, or
   * for `maxIterations` iterations.
   *
   * @tparam VD the vertex data type
   * @tparam ED the edge data type
   * @tparam A the Pregel message type
   *
   * @param graph the input graph.
   *
   * @param initialMsg the message each vertex will receive at the on
   * the first iteration
   *
   * @param maxIterations the maximum number of iterations to run for
   *
   * @param activeDirection the direction of edges incident to a vertex that received a message in
   * the previous round on which to run `sendMsg`. For example, if this is `EdgeDirection.Out`, only
   * out-edges of vertices that received a message in the previous round will run. The default is
   * `EdgeDirection.Either`, which will run `sendMsg` on edges where either side received a message
   * in the previous round. If this is `EdgeDirection.Both`, `sendMsg` will only run on edges where
   * *both* vertices received a message.
   *
   * @param vprog the user-defined vertex program which runs on each
   * vertex and receives the inbound message and computes a new vertex
   * value.  On the first iteration the vertex program is invoked on
   * all vertices and is passed the default message.  On subsequent
   * iterations the vertex program is only invoked on those vertices
   * that receive messages.
   *
   * @param sendMsg a user supplied function that is applied to out
   * edges of vertices that received messages in the current
   * iteration
   *
   * @param mergeMsg a user supplied function that takes two incoming
   * messages of type A and merges them into a single message of type
   * A.  ''This function must be commutative and associative and
   * ideally the size of A should not increase.''
   *
   * @return the resulting graph at the end of the computation
   *
   */
  def run[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: Graph[VD, ED],
   maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vertexProgram: (VertexId, VD, Option[A], VertexContext) => VD,
   sendMsg: (EdgeTriplet[VD, ED], EdgeContext) => Iterator[(VertexId, A)],
   mergeMsg: (A, A) => A)
  : Graph[VD, ED] =
  {
    // Initialize the graph with all vertices active
    var g: Graph[(VD, Boolean), ED] = graph.mapVertices { (vid, vdata) => (vdata, true) }.cache()
    // Determine the set of vertices that did not vote to halt
    var activeVertices = g.vertices
    var numActive = activeVertices.count()
    var i = 0
    while (numActive > 0 && i < maxIterations) {
      // The send message wrapper removes the active fields from the triplet and places them in the edge context.
      def sendMessageWrapper(triplet: EdgeTriplet[(VD, Boolean),ED]): Iterator[(VertexId, A)] = {
        val simpleTriplet = new EdgeTriplet[VD, ED]()
        simpleTriplet.set(triplet)
        simpleTriplet.srcAttr = triplet.srcAttr._1
        simpleTriplet.dstAttr = triplet.dstAttr._1
        val ctx = new EdgeContext(i, triplet.srcAttr._2, triplet.dstAttr._2)
        sendMsg(simpleTriplet, ctx)
      }

      // Compute the messages for all the active vertices
      val messages = g.mapReduceTriplets(sendMessageWrapper, mergeMsg, Some((activeVertices, activeDirection)))

      // get a reference to the current graph so that we can unpersist it once the new graph is created.
      val prevG = g

      // Receive the messages to the subset of active vertices
      g = g.outerJoinVertices(messages){ (vid, dataAndActive, msgOpt) =>
        val (vdata, active) = dataAndActive
        // If the vertex voted to halt and received no message then we can skip the vertex program
        if (!active && msgOpt.isEmpty) {
          dataAndActive
        } else {
          val ctx = new VertexContext(i, active)
          // The vertex program is either active or received a message (or both).
          // A vertex program should vote to halt again even if it has previously voted to halt
          (vertexProgram(vid, vdata, msgOpt, ctx), ctx.isActive)
        }
      }.cache()

      // Recompute the active vertices (those that have not voted to halt)
      activeVertices = g.vertices.filter(v => v._2._2)

      // Force all computation!
      numActive = activeVertices.count()

      // Unpersist the RDDs hidden by newly-materialized RDDs
      prevG.unpersistVertices(blocking=false)
      prevG.edges.unpersist(blocking=false)

      logInfo("Pregel finished iteration " + i)
      // count the iteration
      i += 1
    }
    g.mapVertices((id, vdata) => vdata._1)
  } // end of apply


} // end of class Pregel
