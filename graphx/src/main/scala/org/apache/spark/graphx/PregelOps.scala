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

import org.apache.spark.Logging

import scala.reflect.ClassTag

/**
 * Contains additional functionality for [[Pregel]] of partially sending message.
 *
 * Execute a Pregel-like iterative vertex-parallel abstraction with current iterative number.
 * Part of the vertexes(called `ActiveVertexes`) send messages to their neighbours
 * in each iteration.
 *
 * In some cases, `ActiveVertexes` are the vertexes that their attributes do not change
 * between the previous and current iteration, so they need not to send message.
 * At first, user can set Int value(eg. `flag:Int`) with `vprog`'s first parameter `curIter`
 * to vertex's attribute in function `vprog`.
 * Then in `sendMsg`, compare the Int value (`flag`) of Vertex attribute with `curIter` of
 * `sendMsg`'s first parameter.
 * In this way, it can determine whether sending message in current iteration.
 *
 * @example sample:
 * {{{
 *
 *   // invoke
 *  PregelOps[(Int, Int), Int, Map[Int, Int]](graph, isTerminal = isTerminal)(
 *    vprog, sendMessage, mergeMessage)
 *
 *  //  set a `flag:Int` value of vertex attribute object
 *  def vprog(curIter: Int, vid: VertexId, attr: (Int, Int),
 *   messages: Map[Int, Int]): (Int, Int) = {
 *   if (attr > 1024) {
 *   //  logic code...
 *   //  assign the curIter, the vertex can send message to its neighbors in sendMsg
 *     (curIter, xxxx)
 *   } else {
 *     (0, xxxx)
 *   }
 *  }
 *
 *  def sendMessage(curIter: Int,
 *    ctx: EdgeContext[(Int, Int), Int, Map[Int, Int]]): Unit = {
 *    if (curIter == 0) {
 *     ctx.sendToDst(Map(ctx.srcAttr._2 -> -1, ctx.srcAttr.xx -> 1))
 *     ctx.sendToSrc(Map(ctx.dstAttr._2 -> -1, ctx.dstAttr.xx -> 1))
 *    } else if (curIter == ctx.srcAttr._1) {
 *     //  determine whether sending message
 *     ctx.sendToDst(Map(ctx.srcAttr.preKCore -> -1, ctx.srcAttr.curKCore -> 1))
 *     ctx.sendToSrc(Map(ctx.dstAttr.preKCore -> -1, ctx.dstAttr.curKCore -> 1))
 *    }
 *   }
 *
 *  def isTerminal(curIter: Int, messageCount: Long): Boolean = {
 *   if (messageCount < 10 || curIter > 1000)  false else  true
 *  }
 *
 *  // mergeMessage
 *  def mergeMessage(source: Map[Int, Int], target: Map[Int, Int]): Map[Int, Int] = {
 *   //  logic code...
 *
 *   target
 *  }
 *
 * }}}
 *
 */
object PregelOps extends Logging {

  /**
   * Implementing Part of the vertexes(we call them ActiveVertexes) send messages to their
   * neighbours in each iteration.
   *
   * Provide a `isTerminal` to determine end up the loop with Int value `curIter` and the number
   * of message count number previous iterate.
   *
   * @tparam VD the vertex data type
   * @tparam ED the edge data type
   * @tparam A the Pregel message type
   *
   * @param originGraph the input graph.
   *
   * @param initialMsg the message each vertex will receive at the on
   * the first iteration. default is [[None]]
   *
   * @param isTerminal checking whether can finish loop
   * Parameter Int is the current iteration variable `curIter`
   * Parameter Long is the aggregate message number of previous iteration
   *
   * @param tripletFields which fields should be included in the [[EdgeContext]] passed to the
   * `sendMsg` function. If not all fields are needed, specifying this can improve performance.
   * default is [[TripletFields.All]]
   *
   * @param vprog the user-defined vertex program which runs on each
   * vertex and receives the inbound message and computes a new vertex
   * value.  On the first iteration the vertex program is invoked on
   * all vertices and is passed the default message.  On subsequent
   * iterations the vertex program is only invoked on those vertices
   * that receive messages.
   *
   * @param sendMsg a user supplied function that is applied to out
   * edges of vertices that received messages in the current iteration
   *
   * @param mergeMsg a user supplied function that takes two incoming
   * messages of type A and merges them into a single message of type A.
   * ''This function must be commutative and associative and
   * ideally the size of A should not increase.''
   *
   * @return the resulting graph at the end of the computation
   */
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (originGraph: Graph[VD, ED],
    initialMsg: Option[A] = None,
    isTerminal: (Int, Long) => Boolean = defaultTerminal,
    tripletFields: TripletFields)
    (vprog: (Int, VertexId, VD, A) => VD,
      sendMsg: (Int, EdgeContext[VD, ED, A]) => Unit,
      mergeMsg: (A, A) => A): Graph[VD, ED] = {

    //  init iterate 0
    val initIter = 0
    var graph = initialMsg match {
      case None => originGraph.cache()
      case _ => originGraph.mapVertices((vid, vdata) => vprog(initIter, vid, vdata,
        initialMsg.get)).cache()
    }

    // compute the messages
    var messageRDD = graph.aggregateMessages(sendMsg(initIter, _: EdgeContext[VD, ED, A]), mergeMsg)
    var activeMsgCount = messageRDD.count()

    // Loop, from i = 1
    var i = 1
    while (activeMsgCount > 0 && isTerminal(i, activeMsgCount)) {
      val ct = System.currentTimeMillis()
      val curIter = i

      // 1. Receive the messages. Vertices that didn't get any messages do not appear in newVerts.
      val newVerts = graph.vertices.innerJoin(messageRDD)(
        vprog(curIter, _: VertexId, _: VD, _: A)).cache()

      // 2. Update the graph with the new vertices.
      val preGraph: Graph[VD, ED] = graph
      graph = graph.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old)}
      graph.cache()

      val oldMessages = messageRDD
      // 3. aggregate message
      // Send new messages. Vertices that didn't get any messages don't appear in newVerts, so don't
      // get to send messages. We must cache messages so it can be materialized on the next line,
      // allowing us to uncache the previous iteration.
      messageRDD = graph.aggregateMessages(sendMsg(curIter, _: EdgeContext[VD, ED, A]), mergeMsg,
        tripletFields).cache()
      // The call to count() materializes `messages`, `newVerts`, and the vertices of `g`. This
      // hides oldMessages (depended on by newVerts), newVerts (depended on by messages), and the
      // vertices of prevG (depended on by newVerts, oldMessages, and the vertices of g).
      activeMsgCount = messageRDD.count()

      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking = false)
      newVerts.unpersist(blocking = false)
      preGraph.unpersistVertices(blocking = false)
      preGraph.edges.unpersist(blocking = false)
      if (i == 1) {
        originGraph.unpersistVertices(blocking = false)
        originGraph.edges.unpersist(blocking = false)
      }

      i += 1

      logInfo("{\"name\":\"pregel\", \"iterate\":" + i + ",\"cost\":"
        + (System.currentTimeMillis() - ct) + "}")
    }

    graph
  } // end of apply

  /**
   * default terminal function
   * @return
   */
  private def defaultTerminal(curIter: Int, msgCount: Long): Boolean = true
}
