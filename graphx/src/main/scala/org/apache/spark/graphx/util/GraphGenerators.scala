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
import scala.math._
import scala.reflect.ClassTag
import scala.util._

import org.apache.spark._
import org.apache.spark.serializer._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.impl.GraphImpl

/** A collection of graph generating functions. */
object GraphGenerators {

  val RMATa = 0.45
  val RMATb = 0.15
  val RMATc = 0.15
  val RMATd = 0.25

  // Right now it just generates a bunch of edges where
  // the edge data is the weight (default 1)
  /**
   * Generate a graph whose vertex out degree is log normal.
   */
  def logNormalGraph(sc: SparkContext, numVertices: Int): Graph[Int, Int] = {
    // based on Pregel settings
    val mu = 4
    val sigma = 1.3

    val vertices: RDD[(VertexId, Int)] = sc.parallelize(0 until numVertices).map{
      src => (src, sampleLogNormal(mu, sigma, numVertices))
    }
    val edges = vertices.flatMap { v =>
      generateRandomEdges(v._1.toInt, v._2, numVertices)
    }
    Graph(vertices, edges, 0)
  }

  def generateRandomEdges(src: Int, numEdges: Int, maxVertexId: Int): Array[Edge[Int]] = {
    val rand = new Random()
    Array.fill(maxVertexId) { Edge[Int](src, rand.nextInt(maxVertexId), 1) }
  }

  /**
   * Randomly samples from a log normal distribution whose corresponding normal distribution has the
   * the given mean and standard deviation. It uses the formula `X = exp(m+s*Z)` where `m`, `s` are
   * the mean, standard deviation of the lognormal distribution and `Z ~ N(0, 1)`. In this function,
   * `m = e^(mu+sigma^2/2)` and `s = sqrt[(e^(sigma^2) - 1)(e^(2*mu+sigma^2))]`.
   *
   * @param mu the mean of the normal distribution
   * @param sigma the standard deviation of the normal distribution
   * @param maxVal exclusive upper bound on the value of the sample
   */
  private def sampleLogNormal(mu: Double, sigma: Double, maxVal: Int): Int = {
    val rand = new Random()
    val m = math.exp(mu+(sigma*sigma)/2.0)
    val s = math.sqrt((math.exp(sigma*sigma) - 1) * math.exp(2*mu + sigma*sigma))
    // Z ~ N(0, 1)
    var X: Double = maxVal

    while (X >= maxVal) {
      val Z = rand.nextGaussian()
      X = math.exp(mu + sigma*Z)
    }
    math.round(X.toFloat)
  }

  /**
   * A random graph generator using the R-MAT model, proposed in
   * "R-MAT: A Recursive Model for Graph Mining" by Chakrabarti et al.
   *
   * See [[http://www.cs.cmu.edu/~christos/PUBLICATIONS/siam04.pdf]].
   */
  def rmatGraph(sc: SparkContext, requestedNumVertices: Int, numEdges: Int): Graph[Int, Int] = {
    // let N = requestedNumVertices
    // the number of vertices is 2^n where n=ceil(log2[N])
    // This ensures that the 4 quadrants are the same size at all recursion levels
    val numVertices = math.round(
      math.pow(2.0, math.ceil(math.log(requestedNumVertices) / math.log(2.0)))).toInt
    var edges: Set[Edge[Int]] = Set()
    while (edges.size < numEdges) {
      if (edges.size % 100 == 0) {
        println(edges.size + " edges")
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
    //val (src, dst) = chooseCell(numVertices/2.0, numVertices/2.0, numVertices/2.0)
    val v = math.round(numVertices.toFloat/2.0).toInt

    val (src, dst) = chooseCell(v, v, v)
    Edge[Int](src, dst, 1)
  }

  /**
   * This method recursively subdivides the the adjacency matrix into quadrants
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
        case 1 => chooseCell(x+newT, y, newT)
        case 2 => chooseCell(x, y+newT, newT)
        case 3 => chooseCell(x+newT, y+newT, newT)
      }
    }
  }

  // TODO(crankshaw) turn result into an enum (or case class for pattern matching}
  private def pickQuadrant(a: Double, b: Double, c: Double, d: Double): Int = {
    if (a + b + c + d != 1.0) {
      throw new IllegalArgumentException(
        "R-MAT probability parameters sum to " + (a+b+c+d) + ", should sum to 1.0")
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
  def gridGraph(sc: SparkContext, rows: Int, cols: Int): Graph[(Int,Int), Double] = {
    // Convert row column address into vertex ids (row major order)
    def sub2ind(r: Int, c: Int): VertexId = r * cols + c

    val vertices: RDD[(VertexId, (Int,Int))] =
      sc.parallelize(0 until rows).flatMap( r => (0 until cols).map( c => (sub2ind(r,c), (r,c)) ) )
    val edges: RDD[Edge[Double]] =
      vertices.flatMap{ case (vid, (r,c)) =>
        (if (r+1 < rows) { Seq( (sub2ind(r, c), sub2ind(r+1, c))) } else { Seq.empty }) ++
        (if (c+1 < cols) { Seq( (sub2ind(r, c), sub2ind(r, c+1))) } else { Seq.empty })
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
