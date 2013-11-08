package org.apache.spark.graph

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import org.apache.spark.graph.LocalSparkContext._

import org.apache.spark.graph.util.GraphGenerators


object GridPageRank {
  def apply(nRows: Int, nCols: Int, nIter: Int, resetProb: Double) = { 
    val inNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])
    val outDegree = Array.fill(nRows * nCols)(0)
    // Convert row column address into vertex ids (row major order)
    def sub2ind(r: Int, c: Int): Int = r * nCols + c 
    // Make the grid graph
    for(r <- 0 until nRows; c <- 0 until nCols){
      val ind = sub2ind(r,c)
      if(r+1 < nRows) {
        outDegree(ind) += 1
        inNbrs(sub2ind(r+1,c)) += ind 
      }
      if(c+1 < nCols) { 
        outDegree(ind) += 1
        inNbrs(sub2ind(r,c+1)) += ind 
      }
    }
    // compute the pagerank
    var pr = Array.fill(nRows * nCols)(resetProb)
    for(iter <- 0 until nIter) {
      val oldPr = pr
      pr = new Array[Double](nRows * nCols)
      for(ind <- 0 until (nRows * nCols)) {
        pr(ind) = resetProb + (1.0 - resetProb) * 
          inNbrs(ind).map( nbr => oldPr(nbr) / outDegree(nbr)).sum
      }
    }
    (0L until (nRows * nCols)).zip(pr)
  }

}


class AnalyticsSuite extends FunSuite with LocalSparkContext {

  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", "org.apache.spark.graph.GraphKryoRegistrator")


  test("Star PageRank") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val nVertices = 100
      val starGraph = GraphGenerators.starGraph(sc, nVertices)
      val resetProb = 0.15
      val prGraph1 = Analytics.pagerank(starGraph, 1, resetProb)
      val prGraph2 = Analytics.pagerank(starGraph, 2, resetProb)
    
      val notMatching = prGraph1.vertices.zipJoin(prGraph2.vertices) { (vid, pr1, pr2) =>
        if (pr1 != pr2) { 1 } else { 0 }
      }.map { case (vid, test) => test }.sum
      assert(notMatching === 0)
      prGraph2.vertices.foreach(println(_))
      val errors = prGraph2.vertices.map{ case (vid, pr) =>
        val correct = (vid > 0 && pr == resetProb) ||
        (vid == 0 && math.abs(pr - (resetProb + (1.0 - resetProb) * (resetProb * (nVertices - 1)) )) < 1.0E-5)
        if ( !correct ) { 1 } else { 0 }
      }
      assert(errors.sum === 0)

      val prGraph3 = Analytics.deltaPagerank(starGraph, 0, resetProb)
      val errors2 = prGraph2.vertices.leftJoin(prGraph3.vertices){ (vid, pr1, pr2Opt) =>
        pr2Opt match {
          case Some(pr2) if(pr1 == pr2) => 0
          case _ => 1
        }
      }.map { case (vid, test) => test }.sum
      assert(errors2 === 0)
    }
  } // end of test Star PageRank



  test("Grid PageRank") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val gridGraph = GraphGenerators.gridGraph(sc, 10, 10)
      val resetProb = 0.15
      val prGraph1 = Analytics.pagerank(gridGraph, 50, resetProb).cache()
      val prGraph2 = Analytics.deltaPagerank(gridGraph, 0.0001, resetProb).cache()
      val error = prGraph1.vertices.zipJoin(prGraph2.vertices) { case (id, a, b) => (a - b) * (a - b) }
        .map { case (id, error) => error }.sum
      prGraph1.vertices.zipJoin(prGraph2.vertices) { (id, a, b) => (a, b, a-b) }.foreach(println(_))
      println(error)
      assert(error < 1.0e-5)
      val pr3: RDD[(Vid, Double)] = sc.parallelize(GridPageRank(10,10, 50, resetProb))
      val error2 = prGraph1.vertices.leftJoin(pr3) { (id, a, bOpt) =>
        val b: Double  = bOpt.get
        (a - b) * (a - b)
      }.map { case (id, error) => error }.sum
      prGraph1.vertices.leftJoin(pr3) { (id, a, b) => (a, b) }.foreach( println(_) )
      println(error2)
      assert(error2 < 1.0e-5)
    }
  } // end of Grid PageRank


  test("Grid Connected Components") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val gridGraph = GraphGenerators.gridGraph(sc, 10, 10)
      val ccGraph = Analytics.connectedComponents(gridGraph).cache()
      val maxCCid = ccGraph.vertices.map { case (vid, ccId) => ccId }.sum
      assert(maxCCid === 0)
    }
  } // end of Grid connected components


  test("Reverse Grid Connected Components") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val gridGraph = GraphGenerators.gridGraph(sc, 10, 10).reverse
      val ccGraph = Analytics.connectedComponents(gridGraph).cache()
      val maxCCid = ccGraph.vertices.map { case (vid, ccId) => ccId }.sum
      assert(maxCCid === 0)
    }
  } // end of Grid connected components


  test("Chain Connected Components") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val chain1 = (0 until 9).map(x => (x, x+1) )
      val chain2 = (10 until 20).map(x => (x, x+1) )
      val rawEdges = sc.parallelize(chain1 ++ chain2, 3).map { case (s,d) => (s.toLong, d.toLong) }
      val twoChains = Graph(rawEdges)
      val ccGraph = Analytics.connectedComponents(twoChains).cache()
      val vertices = ccGraph.vertices.collect
      for ( (id, cc) <- vertices ) {
        if(id < 10) { assert(cc === 0) }
        else { assert(cc === 10) }
      }
      val ccMap = vertices.toMap
      println(ccMap)
      for( id <- 0 until 20 ) {
        if(id < 10) { assert(ccMap(id) === 0) }
        else { assert(ccMap(id) === 10) }
      }
    }
  } // end of chain connected components

  test("Reverse Chain Connected Components") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val chain1 = (0 until 9).map(x => (x, x+1) )
      val chain2 = (10 until 20).map(x => (x, x+1) )
      val rawEdges = sc.parallelize(chain1 ++ chain2, 3).map { case (s,d) => (s.toLong, d.toLong) }
      val twoChains = Graph(rawEdges).reverse
      val ccGraph = Analytics.connectedComponents(twoChains).cache()
      val vertices = ccGraph.vertices.collect
      for ( (id, cc) <- vertices ) {
        if(id < 10) { assert(cc === 0) }
        else { assert(cc === 10) }
      }
      val ccMap = vertices.toMap
      println(ccMap)
      for( id <- 0 until 20 ) {
        if(id < 10) { assert(ccMap(id) === 0) }
        else { assert(ccMap(id) === 10) }
      }
    }
  } // end of chain connected components



} // end of AnalyticsSuite
