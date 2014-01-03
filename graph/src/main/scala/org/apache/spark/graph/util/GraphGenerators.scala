package org.apache.spark.graph.util

import scala.annotation.tailrec
import scala.math._
import scala.reflect.ClassTag
import scala.util._

import org.apache.spark._
import org.apache.spark.serializer._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graph._
import org.apache.spark.graph.Graph
import org.apache.spark.graph.Edge
import org.apache.spark.graph.impl.GraphImpl

/**
 * @todo cleanup and modularize code
 */
object GraphGenerators {

  val RMATa = 0.45
  val RMATb = 0.15
  val RMATc = 0.15
  val RMATd = 0.25

  def main(args: Array[String]) {


    val serializer = "org.apache.spark.serializer.KryoSerializer"
    System.setProperty("spark.serializer", serializer)
    //System.setProperty("spark.shuffle.compress", "false")
    System.setProperty("spark.kryo.registrator", "spark.graph.GraphKryoRegistrator")
    val host = "local[4]"
    val sc = new SparkContext(host, "Lognormal graph generator")

    val lnGraph = logNormalGraph(sc, 10000)

    val rmat = rmatGraph(sc, 1000, 3000)

    //for (v <- lnGraph.vertices) {
    //  println(v.id + ":\t" + v.data)
    //}

    val times = 100000
    //val nums = (1 to times).flatMap { n => List(sampleLogNormal(4.0, 1.3, times)) }.toList
    //val avg = nums.sum / nums.length
    //val sumSquares = nums.foldLeft(0.0) {(total, next) =>
    //  (total + math.pow((next - avg), 2)) }
    //val stdev = math.sqrt(sumSquares/(nums.length - 1))

    //println("avg: " + avg + "+-" + stdev)


    //for (i <- 1 to 1000) {
    //  println(sampleLogNormal(4.0, 1.3, 1000))
    //}

    sc.stop()

  }


  // Right now it just generates a bunch of edges where
  // the edge data is the weight (default 1)
  def logNormalGraph(sc: SparkContext, numVertices: Int): Graph[Int, Int] = {
    // based on Pregel settings
    val mu = 4
    val sigma = 1.3
    //val vertsAndEdges = (0 until numVertices).flatMap { src => {

    val vertices: RDD[(Vid, Int)] = sc.parallelize(0 until numVertices).map{
      src => (src, sampleLogNormal(mu, sigma, numVertices))
    }

    val edges = vertices.flatMap{
      v => generateRandomEdges(v._1.toInt, v._2, numVertices)
    }

    Graph(vertices, edges, 0)
    //println("Vertices:")
    //for (v <- vertices) {
    //  println(v.id)
    //}

    //println("Edges")
    //for (e <- edges) {
    //  println(e.src, e.dst, e.data)
    //}

  }


  def generateRandomEdges(src: Int, numEdges: Int, maxVid: Int): Array[Edge[Int]] = {
    val rand = new Random()
    var dsts: Set[Int] = Set()
    while (dsts.size < numEdges) {
      val nextDst = rand.nextInt(maxVid)
      if (nextDst != src) {
        dsts += nextDst
      }
    }
    dsts.map {dst => Edge[Int](src, dst, 1) }.toArray
  }


  /**
   * Randomly samples from a log normal distribution
   * whose corresponding normal distribution has the
   * the given mean and standard deviation. It uses
   * the formula X = exp(m+s*Z) where m, s are the
   * mean, standard deviation of the lognormal distribution
   * and Z~N(0, 1). In this function,
   * m = e^(mu+sigma^2/2) and
   * s = sqrt[(e^(sigma^2) - 1)(e^(2*mu+sigma^2))].
   *
   * @param mu the mean of the normal distribution
   * @param sigma the standard deviation of the normal distribution
   * @param macVal exclusive upper bound on the value of the sample
   */
  def sampleLogNormal(mu: Double, sigma: Double, maxVal: Int): Int = {
    val rand = new Random()
    val m = math.exp(mu+(sigma*sigma)/2.0)
    val s = math.sqrt((math.exp(sigma*sigma) - 1) * math.exp(2*mu + sigma*sigma))
    // Z ~ N(0, 1)
    var X: Double = maxVal

    while (X >= maxVal) {
      val Z = rand.nextGaussian()
      //X = math.exp((m + s*Z))
      X = math.exp((mu + sigma*Z))
    }
    math.round(X.toFloat)
  }



  def rmatGraph(sc: SparkContext, requestedNumVertices: Int, numEdges: Int): Graph[Int, Int] = {
    // let N = requestedNumVertices
    // the number of vertices is 2^n where n=ceil(log2[N])
    // This ensures that the 4 quadrants are the same size at all recursion levels
    val numVertices = math.round(math.pow(2.0, math.ceil(math.log(requestedNumVertices)/math.log(2.0)))).toInt
    var edges: Set[Edge[Int]] = Set()
    while (edges.size < numEdges) {
      if (edges.size % 100 == 0) {
        println(edges.size + " edges")
      }
      edges += addEdge(numVertices)

    }
    val graph = outDegreeFromEdges(sc.parallelize(edges.toList))
    graph

  }

  def outDegreeFromEdges[ED: ClassTag](edges: RDD[Edge[ED]]): Graph[Int, ED] = {

    val vertices = edges.flatMap { edge => List((edge.srcId, 1)) }
      .reduceByKey(_ + _)
      .map{ case (vid, degree) => (vid, degree) }
    Graph(vertices, edges, 0)
  }

  /**
   * @param numVertices Specifies the total number of vertices in the graph (used to get
   * the dimensions of the adjacency matrix
   */
  def addEdge(numVertices: Int): Edge[Int] = {
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
   *
   * where this represents the subquadrant of the adj matrix currently being
   * subdivided. (x,y) represent the upper left hand corner of the subquadrant,
   * and T represents the side length (guaranteed to be a power of 2).
   *
   * After choosing the next level subquadrant, we get the resulting sets
   * of parameters:
   *    quad = a, x'=x, y'=y, T'=T/2
   *    quad = b, x'=x+T/2, y'=y, T'=T/2
   *    quad = c, x'=x, y'=y+T/2, T'=T/2
   *    quad = d, x'=x+T/2, y'=y+T/2, T'=T/2
   *
   * @param src is the
   */
  @tailrec
  def chooseCell(x: Int, y: Int, t: Int): (Int, Int) = {
    if (t <= 1)
      (x,y)
    else {
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
  def pickQuadrant(a: Double, b: Double, c: Double, d: Double): Int = {
    if (a+b+c+d != 1.0) {
      throw new IllegalArgumentException("R-MAT probability parameters sum to " + (a+b+c+d) + ", should sum to 1.0")
    }
    val rand = new Random()
    val result = rand.nextDouble()
    result match {
      case x if x < a => 0 // 0 corresponds to quadrant a
      case x if (x >= a && x < a+b) => 1 // 1 corresponds to b
      case x if (x >= a+b && x < a+b+c) => 2 // 2 corresponds to c
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
    def sub2ind(r: Int, c: Int): Vid = r * cols + c

    val vertices: RDD[(Vid, (Int,Int))] =
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
    val edges: RDD[(Vid, Vid)] = sc.parallelize(1 until nverts).map(vid => (vid, 0))
    Graph.fromEdgeTuples(edges, 1)
  } // end of starGraph



} // end of Graph Generators
