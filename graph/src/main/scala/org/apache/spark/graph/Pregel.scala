package org.apache.spark.graph

import org.apache.spark.rdd.RDD


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
 * This object present several variants of the bulk synchronous
 * execution that differ only in the edge direction along which
 * messages are sent and whether a fixed number of iterations is used.
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
   * This function iterates a fixed number (`numIter`) of iterations.
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
   * @param numIter the number of iterations to run this computation.
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
  def apply[VD: ClassManifest, ED: ClassManifest, A: ClassManifest]
    (graph: Graph[VD, ED], initialMsg: A, numIter: Int)(
      vprog: (Vid, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Array[(Vid,A)],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED] = {

    // Receive the first set of messages
    var g = graph.mapVertices( (vid, vdata) => vprog(vid, vdata, initialMsg)).cache

    var i = 0
    while (i < numIter) {
      // compute the messages
      val messages = g.mapReduceTriplets(sendMsg, mergeMsg)
      // receive the messages
      g = g.joinVertices(messages)(vprog).cache
      // count the iteration
      i += 1
    }
    // Return the final graph
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
   * This function iterates until there are no remaining messages.
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
   * @param numIter the number of iterations to run this computation.
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
  def apply[VD: ClassManifest, ED: ClassManifest, A: ClassManifest]
    (graph: Graph[VD, ED], initialMsg: A)(
      vprog: (Vid, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Array[(Vid,A)],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED] = {

    def vprogFun(id: Vid, attr: (VD, Boolean), msgOpt: Option[A]): (VD, Boolean) = {
      msgOpt match {
        case Some(msg) => (vprog(id, attr._1, msg), true)
        case None => (attr._1, false)
      }
    }

    def sendMsgFun(edge: EdgeTriplet[(VD,Boolean), ED]): Array[(Vid, A)] = {
      if(edge.srcAttr._2) {
        val et = new EdgeTriplet[VD, ED]
        et.srcId = edge.srcId
        et.srcAttr = edge.srcAttr._1
        et.dstId = edge.dstId
        et.dstAttr = edge.dstAttr._1
        et.attr = edge.attr
        sendMsg(et)
      } else { Array.empty[(Vid,A)] }
    }

    var g = graph.mapVertices( (vid, vdata) => (vprog(vid, vdata, initialMsg), true) )
    // compute the messages
    var messages = g.mapReduceTriplets(sendMsgFun, mergeMsg).cache
    var activeMessages = messages.count
    // Loop
    var i = 0
    while (activeMessages > 0) {
      // receive the messages
      g = g.outerJoinVertices(messages)(vprogFun)
      val oldMessages = messages
      // compute the messages
      messages = g.mapReduceTriplets(sendMsgFun, mergeMsg).cache
      activeMessages = messages.count
      // after counting we can unpersist the old messages
      oldMessages.unpersist(blocking=false)
      // count the iteration
      i += 1
    }
    // Return the final graph
    g.mapVertices((id, attr) => attr._1)
  } // end of apply

} // end of class Pregel
