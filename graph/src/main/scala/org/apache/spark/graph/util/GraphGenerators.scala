package org.apache.spark.graph.util

import util.Random.nextGaussian
import math._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graph.Graph
import org.apache.spark.graph.Vertex
import org.apache.spark.graph.Edge

object GraphGenerator {

  /*
    TODO(crankshaw) delete
    Just notes for me:
      for every vertex:
        generate the number of outdegrees
        create the vertex Vertex(vid, outdegrees)
        create the edges: generateRandomEdges
        add vertex to vertex list
        add edges to edgelist

  */
  def main(args: Array[String]) {
    println("hello world")

  }
  
  // For now just writes graph to a file. Eventually
  // it will return a spark.graph.Graph


  // Right now it just generates a bunch of edges where
  // the edge data is the weight (default 1)
  def lognormalGraph(numVertices: Long, fname: String) = {
    // based on Pregel settings
    val mu = 4
    val sigma = 1.3
    val vertsAndEdges = Range(0, numVertices).flatmap { src => {
      val outdegree = sampleLogNormal(mu, sigma, numVertices)
      val vertex = Vertex(src, outdegree)
      val edges = generateRandomEdges(src, outdegree, numVertices)
      (vertex, edges) }
    }
    val vertices, edges = vertsAndEdges.unzip
    val graph = new GraphImpl[Int, Int](vertices, edges.flatten)
  }

  def generateRandomEdges(src: Long, numEdges: Long, maxVid): Array[Edge[Int]] = {
    var dsts = new Set()
    while (dsts.size() < numEdges) {
      val nextDst = nextInt(maxVid)
      if (nextDst != src) {
        dsts += nextDst
      }
    }
    val edges = dsts.map(dst => Array(Edge(src, dst, 1))).toList
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
  def sampleLogNormal(mu: Float, sigma: Float, maxVal: Long): Long = {
    val m = math.exp(mu+(sigma*sigma)/2.0)
    val s = math.sqrt((math.exp(sigma*sigma) - 1) * math.exp(2*mu + sigma*sigma))
    // Z ~ N(0, 1)
    var X = maxVal
    while (X >= maxVal) {
      val Z = nextGaussian()
      X = math.exp(m + s*Z)
    }
    math.round(X)
  }

}
