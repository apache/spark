package org.apache.spark.graph

import scala.reflect.ClassTag


/**
 * This object implements a Pregel-like bulk-synchronous
 * message-passing API.  However, unlike the original Pregel API the
 * GraphX pregel API factors the sendMessage computation over edges,
 * enables the message sending computation to read both vertex
 * attributes, and finally constrains messages to the graph structure.
 * These changes allow for substantially more efficient distributed
 * execution while also exposing greater flexibility for graph based
 * computation.
 *
 * @example We can use the Pregel abstraction to implement PageRank
 * {{{
 * val pagerankGraph: Graph[Double, Double] = graph
 *   // Associate the degree with each vertex
 *   .outerJoinVertices(graph.outDegrees){
 *     (vid, vdata, deg) => deg.getOrElse(0)
 *   }
 *   // Set the weight on the edges based on the degree
 *   .mapTriplets( e => 1.0 / e.srcAttr )
 *   // Set the vertex attributes to the initial pagerank values
 *   .mapVertices( (id, attr) => 1.0 )
 *
 * def vertexProgram(id: Vid, attr: Double, msgSum: Double): Double =
 *   resetProb + (1.0 - resetProb) * msgSum
 * def sendMessage(id: Vid, edge: EdgeTriplet[Double, Double]): Option[Double] =
 *   Some(edge.srcAttr * edge.attr)
 * def messageCombiner(a: Double, b: Double): Double = a + b
 * val initialMessage = 0.0
 * // Execute pregel for a fixed number of iterations.
 * Pregel(pagerankGraph, initialMessage, numIter)(
 *   vertexProgram, sendMessage, messageCombiner)
 * }}}
 *
 */
object Pregel {

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
   * for maxIterations iterations.
   *
   * @tparam VD the vertex data type
   * @tparam ED the edge data type
   * @tparam A the Pregel message type
   *
   * @param graph the input graph.
   *
   * @param initialMsg the message each vertex will receive at the on
   * the first iteration.
   *
   * @param maxIterations the maximum number of iterations to run for.
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
   * iteration.
   *
   * @param mergeMsg a user supplied function that takes two incoming
   * messages of type A and merges them into a single message of type
   * A.  ''This function must be commutative and associative and
   * ideally the size of A should not increase.''
   *
   * @return the resulting graph at the end of the computation
   *
   */
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
    (graph: Graph[VD, ED], initialMsg: A, maxIterations: Int = Int.MaxValue)(
      vprog: (Vid, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(Vid,A)],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED] = {

    var g = graph.mapVertices( (vid, vdata) => vprog(vid, vdata, initialMsg) )
    // compute the messages
    var messages = g.mapReduceTriplets(sendMsg, mergeMsg).cache()
    var activeMessages = messages.count()
    // Loop
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      // Receive the messages. Vertices that didn't get any messages do not appear in newVerts.
      val newVerts = g.vertices.innerJoin(messages)(vprog).cache()
      // Update the graph with the new vertices.
      g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }

      val oldMessages = messages
      // Send new messages. Vertices that didn't get any messages don't appear in newVerts, so don't
      // get to send messages.
      messages = g.mapReduceTriplets(sendMsg, mergeMsg, Some((newVerts, EdgeDirection.Out))).cache()
      activeMessages = messages.count()
      // after counting we can unpersist the old messages
      oldMessages.unpersist(blocking=false)
      // count the iteration
      i += 1
    }

    g
  } // end of apply

} // end of class Pregel
