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

package org.apache.spark.graphx.util

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util._

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

/** A collection of graph generating functions. */
object GraphGenerators extends Logging {

  val RMATa = 0.45
  val RMATb = 0.15
  val RMATd = 0.25

  /**
   * Generate a graph whose vertex out degree distribution is log normal.
   *
   * The default values for mu and sigma are taken from the Pregel paper:
   *
   * Grzegorz Malewicz, Matthew H. Austern, Aart J.C Bik, James C. Dehnert,
   * Ilan Horn, Naty Leiser, and Grzegorz Czajkowski. 2010.
   * Pregel: a system for large-scale graph processing. SIGMOD '10.
   *
   * If the seed is -1 (default), a random seed is chosen. Otherwise, use
   * the user-specified seed.
   *
   * @param sc Spark Context
   * @param numVertices number of vertices in generated graph
   * @param numEParts (optional) number of partitions
   * @param mu (optional, default: 4.0) mean of out-degree distribution
   * @param sigma (optional, default: 1.3) standard deviation of out-degree distribution
   * @param seed (optional, default: -1) seed for RNGs, -1 causes a random seed to be chosen
   * @return Graph object
   */
  def logNormalGraph(
      sc: SparkContext, numVertices: Int, numEParts: Int = 0, mu: Double = 4.0,
      sigma: Double = 1.3, seed: Long = -1): Graph[Long, Int] = {

    val evalNumEParts = if (numEParts == 0) sc.defaultParallelism else numEParts

    // Enable deterministic seeding
    val seedRand = if (seed == -1) new Random() else new Random(seed)
    val seed1 = seedRand.nextInt()
    val seed2 = seedRand.nextInt()

    val vertices: RDD[(VertexId, Long)] = sc.parallelize(0 until numVertices, evalNumEParts).map {
      src => (src, sampleLogNormal(mu, sigma, numVertices, seed = (seed1 ^ src)))
    }

    val edges = vertices.flatMap { case (src, degree) =>
      generateRandomEdges(src.toInt, degree.toInt, numVertices, seed = (seed2 ^ src))
    }

    Graph(vertices, edges, 0)
  }

  // Right now it just generates a bunch of edges where
  // the edge data is the weight (default 1)
  val RMATc = 0.15

  def generateRandomEdges(
      src: Int, numEdges: Int, maxVertexId: Int, seed: Long = -1): Array[Edge[Int]] = {
    val rand = if (seed == -1) new Random() else new Random(seed)
    Array.fill(numEdges) { Edge[Int](src, rand.nextInt(maxVertexId), 1) }
  }

  /**
   * Randomly samples from the given mean and standard deviation of the normal distribution.
   * It uses the formula `X = exp(mu+sigma*Z)` where `mu`,
   * `sigma` are the mean, standard deviation of the normal distribution and
   * `Z ~ N(0, 1)`.
   *
   * @param mu the mean of the normal distribution
   * @param sigma the standard deviation of the normal distribution
   * @param maxVal exclusive upper bound on the value of the sample
   * @param seed optional seed
   */
  private[spark] def sampleLogNormal(
      mu: Double, sigma: Double, maxVal: Int, seed: Long = -1): Int = {
    val rand = if (seed == -1) new Random() else new Random(seed)

    // Z ~ N(0, 1)
    var X: Double = maxVal

    while (X >= maxVal) {
      val Z = rand.nextGaussian()
      X = math.exp(mu + sigma*Z)
    }
    math.floor(X).toInt
  }

  /**
   * A random graph generator using the R-MAT model, proposed in
   * "R-MAT: A Recursive Model for Graph Mining" by Chakrabarti et al.
   *
   * See http://www.cs.cmu.edu/~christos/PUBLICATIONS/siam04.pdf.
   */
  def rmatGraph(sc: SparkContext, requestedNumVertices: Int, numEdges: Int): Graph[Int, Int] = {
    // let N = requestedNumVertices
    // the number of vertices is 2^n where n=ceil(log2[N])
    // This ensures that the 4 quadrants are the same size at all recursion levels
    val numVertices = math.round(
      math.pow(2.0, math.ceil(math.log(requestedNumVertices) / math.log(2.0)))).toInt
    val numEdgesUpperBound =
      math.pow(2.0, 2 * ((math.log(numVertices) / math.log(2.0)) - 1)).toInt
    if (numEdgesUpperBound < numEdges) {
      throw new IllegalArgumentException(
        s"numEdges must be <= $numEdgesUpperBound but was $numEdges")
    }
    val edges = mutable.Set.empty[Edge[Int]]
    while (edges.size < numEdges) {
      if (edges.size % 100 == 0) {
        logDebug(edges.size + " edges")
      }
      edges += addEdge(numVertices)
    }
    outDegreeFromEdges(sc.parallelize(edges.toList))
  }

  private def outDegreeFromEdges[ED: ClassTag](edges: RDD[Edge[ED]]): Graph[Int, ED] = {
    val vertices = edges.flatMap { edge => List((edge.srcId, 1)) }
      .reduceByKey(_ + _)
      .map{ case (vid, degree) => (vid, degree) }
    Graph(vertices, edges, 0)
  }

  /**
   * @param numVertices Specifies the total number of vertices in the graph (used to get
   * the dimensions of the adjacency matrix
   */
  private def addEdge(numVertices: Int): Edge[Int] = {
    // val (src, dst) = chooseCell(numVertices/2.0, numVertices/2.0, numVertices/2.0)
    val v = math.round(numVertices.toFloat/2.0).toInt

    val (src, dst) = chooseCell(v, v, v)
    Edge[Int](src, dst, 1)
  }

  /**
   * This method recursively subdivides the adjacency matrix into quadrants
   * until it picks a single cell. The naming conventions in this paper match
   * those of the R-MAT paper. There are a power of 2 number of nodes in the graph.
   * The adjacency matrix looks like:
   * <pre>
   *
   *          dst ->
   * (x,y) ***************  _
   *       |      |      |  |
   *       |  a   |  b   |  |
   *  src  |      |      |  |
   *   |   ***************  | T
   *  \|/  |      |      |  |
   *       |   c  |   d  |  |
   *       |      |      |  |
   *       ***************  -
   * </pre>
   *
   * where this represents the subquadrant of the adj matrix currently being
   * subdivided. (x,y) represent the upper left hand corner of the subquadrant,
   * and T represents the side length (guaranteed to be a power of 2).
   *
   * After choosing the next level subquadrant, we get the resulting sets
   * of parameters:
   * {{{
   *    quad = a, x'=x, y'=y, T'=T/2
   *    quad = b, x'=x+T/2, y'=y, T'=T/2
   *    quad = c, x'=x, y'=y+T/2, T'=T/2
   *    quad = d, x'=x+T/2, y'=y+T/2, T'=T/2
   * }}}
   */
  @tailrec
  private def chooseCell(x: Int, y: Int, t: Int): (Int, Int) = {
    if (t <= 1) {
      (x, y)
    } else {
      val newT = math.round(t.toFloat/2.0).toInt
      pickQuadrant(RMATa, RMATb, RMATc, RMATd) match {
        case 0 => chooseCell(x, y, newT)
        case 1 => chooseCell(x + newT, y, newT)
        case 2 => chooseCell(x, y + newT, newT)
        case 3 => chooseCell(x + newT, y + newT, newT)
      }
    }
  }

  private def pickQuadrant(a: Double, b: Double, c: Double, d: Double): Int = {
    if (a + b + c + d != 1.0) {
      throw new IllegalArgumentException("R-MAT probability parameters sum to " + (a + b + c + d)
        + ", should sum to 1.0")
    }
    val rand = new Random()
    val result = rand.nextDouble()
    result match {
      case x if x < a => 0 // 0 corresponds to quadrant a
      case x if (x >= a && x < a + b) => 1 // 1 corresponds to b
      case x if (x >= a + b && x < a + b + c) => 2 // 2 corresponds to c
      case _ => 3 // 3 corresponds to d
    }
  }

  /**
   * Create `rows` by `cols` grid graph with each vertex connected to its
   * row+1 and col+1 neighbors.  Vertex ids are assigned in row major
   * order.
   *
   * @param sc the spark context in which to construct the graph
   * @param rows the number of rows
   * @param cols the number of columns
   *
   * @return A graph containing vertices with the row and column ids
   * as their attributes and edge values as 1.0.
   */
  def gridGraph(sc: SparkContext, rows: Int, cols: Int): Graph[(Int, Int), Double] = {
    // Convert row column address into vertex ids (row major order)
    def sub2ind(r: Int, c: Int): VertexId = r * cols + c

    val vertices: RDD[(VertexId, (Int, Int))] = sc.parallelize(0 until rows).flatMap { r =>
      (0 until cols).map( c => (sub2ind(r, c), (r, c)) )
    }
    val edges: RDD[Edge[Double]] =
      vertices.flatMap{ case (vid, (r, c)) =>
        (if (r + 1 < rows) { Seq( (sub2ind(r, c), sub2ind(r + 1, c))) } else { Seq.empty }) ++
        (if (c + 1 < cols) { Seq( (sub2ind(r, c), sub2ind(r, c + 1))) } else { Seq.empty })
      }.map{ case (src, dst) => Edge(src, dst, 1.0) }
    Graph(vertices, edges)
  } // end of gridGraph

  /**
   * Create a star graph with vertex 0 being the center.
   *
   * @param sc the spark context in which to construct the graph
   * @param nverts the number of vertices in the star
   *
   * @return A star graph containing `nverts` vertices with vertex 0
   * being the center vertex.
   */
  def starGraph(sc: SparkContext, nverts: Int): Graph[Int, Int] = {
    val edges: RDD[(VertexId, VertexId)] = sc.parallelize(1 until nverts).map(vid => (vid, 0))
    Graph.fromEdgeTuples(edges, 1)
  } // end of starGraph

} // end of Graph Generators
