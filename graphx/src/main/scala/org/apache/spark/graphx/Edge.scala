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

import org.apache.spark.util.collection.SortDataFormat

/**
 * A single directed edge consisting of a source id, target id,
 * and the data associated with the edge.
 *
 * @tparam ED type of the edge attribute
 *
 * @param srcId The vertex id of the source vertex
 * @param dstId The vertex id of the target vertex
 * @param attr The attribute associated with the edge
 */
case class Edge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] (
    var srcId: VertexId = 0,
    var dstId: VertexId = 0,
    var attr: ED = null.asInstanceOf[ED])
  extends Serializable {

  /**
   * Given one vertex in the edge return the other vertex.
   *
   * @param vid the id one of the two vertices on the edge.
   * @return the id of the other vertex on the edge.
   */
  def otherVertexId(vid: VertexId): VertexId =
    if (srcId == vid) dstId else { assert(dstId == vid); srcId }

  /**
   * Return the relative direction of the edge to the corresponding
   * vertex.
   *
   * @param vid the id of one of the two vertices in the edge.
   * @return the relative direction of the edge to the corresponding
   * vertex.
   */
  def relativeDirection(vid: VertexId): EdgeDirection =
    if (vid == srcId) EdgeDirection.Out else { assert(vid == dstId); EdgeDirection.In }
}

object Edge {
  private[graphx] def lexicographicOrdering[ED] = new Ordering[Edge[ED]] {
    override def compare(a: Edge[ED], b: Edge[ED]): Int = {
      if (a.srcId == b.srcId) {
        if (a.dstId == b.dstId) 0
        else if (a.dstId < b.dstId) -1
        else 1
      } else if (a.srcId < b.srcId) -1
      else 1
    }
  }

  private[graphx] def edgeArraySortDataFormat[ED] = new SortDataFormat[Edge[ED], Array[Edge[ED]]] {
    override def getKey(data: Array[Edge[ED]], pos: Int): Edge[ED] = {
      data(pos)
    }

    override def swap(data: Array[Edge[ED]], pos0: Int, pos1: Int): Unit = {
      val tmp = data(pos0)
      data(pos0) = data(pos1)
      data(pos1) = tmp
    }

    override def copyElement(
        src: Array[Edge[ED]], srcPos: Int,
        dst: Array[Edge[ED]], dstPos: Int): Unit = {
      dst(dstPos) = src(srcPos)
    }

    override def copyRange(
        src: Array[Edge[ED]], srcPos: Int,
        dst: Array[Edge[ED]], dstPos: Int, length: Int): Unit = {
      System.arraycopy(src, srcPos, dst, dstPos, length)
    }

    override def allocate(length: Int): Array[Edge[ED]] = {
      new Array[Edge[ED]](length)
    }
  }
}
