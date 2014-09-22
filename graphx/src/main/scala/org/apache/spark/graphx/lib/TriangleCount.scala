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
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D

/**
 * Compute the number of triangles passing through each vertex.
 *
 * The algorithm is relatively straightforward and can be computed in three steps:
 *
 * <ul>
 * <li>Compute the set of neighbors for each vertex
 * <li>For each edge compute the intersection of the sets and send the count to both vertices.
 * <li> Compute the sum at each vertex and divide by two since each triangle is counted twice.
 * </ul>
 *
 * Note that the input graph should have its edges in canonical direction
 * (i.e. the `sourceId` less than `destId`). Also the graph must have been partitioned
 * using [[org.apache.spark.graphx.Graph#partitionBy]].
 */
object TriangleCount {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): Graph[Int, ED] = {

    // Remove self edges and orient remaining edges from low id to high id
    val canonicalEdges = graph.edges.filter(e => e.srcId != e.dstId).map { e =>
      if (e.srcId < e.dstId) (e.srcId, e.dstId) else (e.dstId, e.srcId)
    }

    // Automatically group duplicate edges
    val reducedGraph = Graph.fromEdgeTuples(canonicalEdges, defaultValue = true,
      uniqueEdges = Some(EdgePartition2D)).cache()

    // Construct set representations of the neighborhoods
    val nbrSets: VertexRDD[VertexSet] =
      reducedGraph.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        val set = new VertexSet(nbrs.length)
        var i = 0
        while (i < nbrs.size) {
          // prevent self cycle
          if(nbrs(i) != vid) {
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }

    // join the sets with the graph
    val setGraph: Graph[VertexSet, Int] = reducedGraph.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }

    // Edge function computes intersection of smaller vertex with larger vertex
    def edgeFunc(ctx: EdgeContext[VertexSet, ED, Int]) {
      val (smallSet, largeSet) = if (ctx.srcAttr.size < ctx.dstAttr.size) {
        (ctx.srcAttr, ctx.dstAttr)
      } else {
        (ctx.dstAttr, ctx.srcAttr)
      }
      val iter = smallSet.iterator
      var counter: Int = 0
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != ctx.srcId && vid != ctx.dstId && largeSet.contains(vid)) {
          counter += 1
        }
      }
      ctx.sendToSrc(counter)
      ctx.sendToDst(counter)
    }

    // compute the intersection along edges
    val counters: VertexRDD[Int] = setGraph.aggregateMessages(edgeFunc, _ + _)
    // Merge counters with the graph and divide by two since each triangle is counted twice
    graph.outerJoinVertices(counters) { (vid, _, optCounter: Option[Int]) =>
      val dblCount = optCounter.getOrElse(0)
      // This algorithm double counts each triangle so the final count should be even
      val isEven = (dblCount & 1) == 0
      if (!isEven) {
        throw new Exception("Triangle count resulted in an invalid number of triangles.")
      }
      dblCount / 2
    }
  }
}
