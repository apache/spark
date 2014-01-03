package org.apache.spark.graph.algorithms

import scala.reflect.ClassTag

import org.apache.spark.graph._


object TriangleCount {
  /**
   * Compute the number of triangles passing through each vertex.
   *
   * The algorithm is relatively straightforward and can be computed in three steps:
   *
   * 1) Compute the set of neighbors for each vertex
   * 2) For each edge compute the intersection of the sets and send the
   *    count to both vertices.
   * 3) Compute the sum at each vertex and divide by two since each
   *    triangle is counted twice.
   *
   *
   * @param graph a graph with `sourceId` less than `destId`. The graph must have been partitioned
   * using Graph.partitionBy.
   *
   * @return
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): Graph[Int, ED] = {
    // Remove redundant edges
    val g = graph.groupEdges((a, b) => a).cache

    // Construct set representations of the neighborhoods
    val nbrSets: VertexRDD[VertexSet] =
      g.collectNeighborIds(EdgeDirection.Both).mapValues { (vid, nbrs) =>
        val set = new VertexSet(4)
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
    val setGraph: Graph[VertexSet, ED] = g.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }
    // Edge function computes intersection of smaller vertex with larger vertex
    def edgeFunc(et: EdgeTriplet[VertexSet, ED]): Iterator[(Vid, Int)] = {
      assert(et.srcAttr != null)
      assert(et.dstAttr != null)
      val (smallSet, largeSet) = if (et.srcAttr.size < et.dstAttr.size) {
        (et.srcAttr, et.dstAttr)
      } else {
        (et.dstAttr, et.srcAttr)
      }
      val iter = smallSet.iterator
      var counter: Int = 0
      while (iter.hasNext) {
        val vid = iter.next
        if (vid != et.srcId && vid != et.dstId && largeSet.contains(vid)) { counter += 1 }
      }
      Iterator((et.srcId, counter), (et.dstId, counter))
    }
    // compute the intersection along edges
    val counters: VertexRDD[Int] = setGraph.mapReduceTriplets(edgeFunc, _ + _)
    // Merge counters with the graph and divide by two since each triangle is counted twice
    g.outerJoinVertices(counters) {
      (vid, _, optCounter: Option[Int]) =>
        val dblCount = optCounter.getOrElse(0)
        // double count should be even (divisible by two)
        assert((dblCount & 1) == 0)
        dblCount / 2
    }

  } // end of TriangleCount

}
