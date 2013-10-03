package org.apache.spark.graph.util

import util._
import math._
//import scala.collection.mutable


import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graph._
import org.apache.spark.graph.Graph
import org.apache.spark.graph.Vertex
import org.apache.spark.graph.Edge
import org.apache.spark.graph.impl.GraphImpl


// TODO(crankshaw) I might want to pull at least RMAT out into a separate class.
// Might simplify the code to have classwide variables and such.
object GraphGenerator {

  val RMATa = 0.45
  val RMATb = 0.15
  val RMATc = 0.15
  val RMATd = 0.25

  /*
  * TODO(crankshaw) delete
  * How do I create a spark context and RDD and stuff?
  * Like how do I actually make this program run?
  */
  def main(args: Array[String]) {


    System.setProperty("spark.serializer", "spark.KryoSerializer")
    //System.setProperty("spark.shuffle.compress", "false")
    System.setProperty("spark.kryo.registrator", "spark.graph.GraphKryoRegistrator")
    val host = "local[4]"
    val sc = new SparkContext(host, "Lognormal graph generator")
    println("hello world")

  }

  // For now just writes graph to a file. Eventually
  // it will return a spark.graph.Graph


  // Right now it just generates a bunch of edges where
  // the edge data is the weight (default 1)
  def lognormalGraph(sc: SparkContext, numVertices: Int): GraphImpl[Int, Int] = {
    // based on Pregel settings
    val mu = 4
    val sigma = 1.3
    //val vertsAndEdges = (0 until numVertices).flatMap { src => {
    val vertices = (0 until numVertices).flatMap { src =>
      Array(Vertex(src, sampleLogNormal(mu, sigma, numVertices))) }
    val edges = vertices.flatMap( { v =>
      generateRandomEdges(v.id.toInt, v.data, numVertices) })
    


    new GraphImpl[Int, Int](sc.parallelize(vertices), sc.parallelize(edges))
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
      X = math.exp((m + s*Z))
    }
    math.round(X.toFloat)
  }



  def rmatGraph(sc: SparkContext, requestedNumVertices: Int, numEdges: Int): GraphImpl[Int, Int] = {
    // let N = requestedNumVertices
    // the number of vertices is 2^n where n=ceil(log2[N])
    // This ensures that the 4 quadrants are the same size at all recursion levels
    val numVertices = math.round(math.pow(2.0, math.ceil(math.log(requestedNumVertices)/math.log(2.0)))).toInt
    var edges: Set[Edge[Int]] = Set()
    while (edges.size < numEdges) {
      edges += addEdge(numVertices)

    }
    val graph = outDegreeFromEdges(sc.parallelize(edges.toList))
    graph

  }

  def outDegreeFromEdges[ED: ClassManifest](edges: RDD[Edge[ED]]): GraphImpl[Int, ED] = {
    
    val vertices = edges.flatMap { edge => List((edge.src, 1)) }
      .reduceByKey(_ + _)
      .map{ case (vid, degree) => Vertex(vid, degree) }
    new GraphImpl[Int, ED](vertices, edges)
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
  @tailrec def chooseCell(x: Int, y: Int, t: Int): (Int, Int) = {
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



}











