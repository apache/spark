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

/**
 * Represents an edge along with its neighboring vertices and allows sending messages along the
 * edge. Used in [[Graph#aggregateMessages]].
 */
abstract class EdgeContext[VD, ED, A] {
  /** The vertex id of the edge's source vertex. */
  def srcId: VertexId
  /** The vertex id of the edge's destination vertex. */
  def dstId: VertexId
  /** The vertex attribute of the edge's source vertex. */
  def srcAttr: VD
  /** The vertex attribute of the edge's destination vertex. */
  def dstAttr: VD
  /** The attribute associated with the edge. */
  def attr: ED

  /** Sends a message to the source vertex. */
  def sendToSrc(msg: A): Unit
  /** Sends a message to the destination vertex. */
  def sendToDst(msg: A): Unit

  /** Converts the edge and vertex properties into an [[EdgeTriplet]] for convenience. */
  def toEdgeTriplet: EdgeTriplet[VD, ED] = {
    val et = new EdgeTriplet[VD, ED]
    et.srcId = srcId
    et.srcAttr = srcAttr
    et.dstId = dstId
    et.dstAttr = dstAttr
    et.attr = attr
    et
  }
}

object EdgeContext {

  /**
   * Extractor mainly used for Graph#aggregateMessages*.
   * Example:
   * {{{
   *  val messages = graph.aggregateMessages(
   *    case ctx @ EdgeContext(_, _, _, _, attr) =>
   *      ctx.sendToDst(attr)
   *    , _ + _)
   * }}}
   */
  def unapply[VD, ED, A](edge: EdgeContext[VD, ED, A]): Some[(VertexId, VertexId, VD, VD, ED)] =
    Some((edge.srcId, edge.dstId, edge.srcAttr, edge.dstAttr, edge.attr))
}
