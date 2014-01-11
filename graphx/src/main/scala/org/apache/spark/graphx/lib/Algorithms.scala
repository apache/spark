package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import org.apache.spark.graphx._

/**
 * Provides graph algorithms directly on [[org.apache.spark.graphx.Graph]] via an implicit
 * conversion.
 * @example
 * {{{
 * import org.apache.spark.graph.lib._
 * val graph: Graph[_, _] = loadGraph()
 * graph.connectedComponents()
 * }}}
 */
class Algorithms[VD: ClassTag, ED: ClassTag](self: Graph[VD, ED]) {
  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @see [[org.apache.spark.graphx.lib.PageRank]], method `runUntilConvergence`.
   */
  def pageRank(tol: Double, resetProb: Double = 0.15): Graph[Double, Double] = {
    PageRank.runUntilConvergence(self, tol, resetProb)
  }

  /**
   * Run PageRank for a fixed number of iterations returning a graph with vertex attributes
   * containing the PageRank and edge attributes the normalized edge weight.
   *
   * @see [[org.apache.spark.graphx.lib.PageRank]], method `run`.
   */
  def staticPageRank(numIter: Int, resetProb: Double = 0.15): Graph[Double, Double] = {
    PageRank.run(self, numIter, resetProb)
  }

  /**
   * Compute the connected component membership of each vertex and return a graph with the vertex
   * value containing the lowest vertex id in the connected component containing that vertex.
   *
   * @see [[org.apache.spark.graphx.lib.ConnectedComponents]]
   */
  def connectedComponents(): Graph[VertexID, ED] = {
    ConnectedComponents.run(self)
  }

  /**
   * Compute the number of triangles passing through each vertex.
   *
   * @see [[org.apache.spark.graphx.lib.TriangleCount]]
   */
  def triangleCount(): Graph[Int, ED] = {
    TriangleCount.run(self)
  }

  /**
   * Compute the strongly connected component (SCC) of each vertex and return a graph with the
   * vertex value containing the lowest vertex id in the SCC containing that vertex.
   *
   * @see [[org.apache.spark.graphx.lib.StronglyConnectedComponents]]
   */
  def stronglyConnectedComponents(numIter: Int): Graph[VertexID, ED] = {
    StronglyConnectedComponents.run(self, numIter)
  }
}
