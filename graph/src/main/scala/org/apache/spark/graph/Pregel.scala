package org.apache.spark.graph

import org.apache.spark.rdd.RDD


/**
 * This object implements the Pregel bulk-synchronous
 * message-passing API.
 */
object Pregel {




  /**
   * Execute the Pregel program.
   *
   * @tparam VD the vertex data type
   * @tparam ED the edge data type
   * @tparam A the Pregel message type
   *
   * @param vprog a user supplied function that acts as the vertex program for
   *              the Pregel computation. It takes the vertex ID of the vertex it is running on,
   *              the accompanying data for that vertex, and the incoming data and returns the
   *              new vertex value.
   * @param sendMsg a user supplied function that takes the current vertex ID and an EdgeTriplet
   *                between the vertex and one of its neighbors and produces a message to send
   *                to that neighbor.
   * @param mergeMsg a user supplied function that takes two incoming messages of type A and merges
   *                 them into a single message of type A. ''This function must be commutative and
   *                 associative.''
   * @param initialMsg the message each vertex will receive at the beginning of the
   *                   first iteration.
   * @param numIter the number of iterations to run this computation for.
   *
   * @return the resulting graph at the end of the computation
   *
   */
  def apply[VD: ClassManifest, ED: ClassManifest, A: ClassManifest]
    (graph: Graph[VD, ED], initialMsg: A, numIter: Int)(
      vprog: (Vid, VD, A) => VD,
      sendMsg: (Vid, EdgeTriplet[VD, ED]) => Option[A],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED] = {

    def mapF(vid: Vid, edge: EdgeTriplet[VD,ED]) = sendMsg(edge.otherVertexId(vid), edge)

    // Receive the first set of messages
    var g = graph.mapVertices( (vid, vdata) => vprog(vid, vdata, initialMsg))
    
    var i = 0
    while (i < numIter) {
      // compute the messages
      val messages = g.aggregateNeighbors(mapF, mergeMsg, EdgeDirection.In)
      // receive the messages
      g = g.joinVertices(messages)(vprog)
      // count the iteration
      i += 1
    }
    // Return the final graph
    g
  } // end of apply


  /**
   * Execute the Pregel program.
   *
   * @tparam VD the vertex data type
   * @tparam ED the edge data type
   * @tparam A the Pregel message type
   *
   * @param vprog a user supplied function that acts as the vertex program for
   *              the Pregel computation. It takes the vertex ID of the vertex it is running on,
   *              the accompanying data for that vertex, and the incoming data and returns the
   *              new vertex value.
   * @param sendMsg a user supplied function that takes the current vertex ID and an EdgeTriplet
   *                between the vertex and one of its neighbors and produces a message to send
   *                to that neighbor.
   * @param mergeMsg a user supplied function that takes two incoming messages of type A and merges
   *                 them into a single message of type A. ''This function must be commutative and
   *                 associative.''
   * @param initialMsg the message each vertex will receive at the beginning of the
   *                   first iteration.
   * @param numIter the number of iterations to run this computation for.
   *
   * @return the resulting graph at the end of the computation
   *
   */
  def apply[VD: ClassManifest, ED: ClassManifest, A: ClassManifest]
    (graph: Graph[VD, ED], initialMsg: A)(
      vprog: (Vid, VD, A) => VD,
      sendMsg: (Vid, EdgeTriplet[VD, ED]) => Option[A],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED] = {

    def vprogFun(id: Vid, attr: (VD, Boolean), msgOpt: Option[A]): (VD, Boolean) = {
      msgOpt match {
        case Some(msg) => (vprog(id, attr._1, msg), true)
        case None => (attr._1, false)
      }
    }

    def sendMsgFun(vid: Vid, edge: EdgeTriplet[(VD,Boolean), ED]): Option[A] = {
      if(edge.srcAttr._2) {
        val et = new EdgeTriplet[VD, ED]
        et.srcId = edge.srcId
        et.srcAttr = edge.srcAttr._1
        et.dstId = edge.dstId
        et.dstAttr = edge.dstAttr._1
        et.attr = edge.attr
        sendMsg(edge.otherVertexId(vid), et)
      } else { None }
    }

    var g = graph.mapVertices( (vid, vdata) => (vprog(vid, vdata, initialMsg), true) ) 
    // compute the messages
    var messages = g.aggregateNeighbors(sendMsgFun, mergeMsg, EdgeDirection.In).cache
    var activeMessages = messages.count
    // Loop 
    var i = 0
    while (activeMessages > 0) {
      // receive the messages
      g = g.outerJoinVertices(messages)(vprogFun)
      val oldMessages = messages
      // compute the messages
      messages = g.aggregateNeighbors(sendMsgFun, mergeMsg, EdgeDirection.In).cache
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
