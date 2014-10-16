/* =============================================================================
#     FileName: Hits.scala
#         Desc: Improved HITS computation in GraphX
#       Author: Lin Wang
#        Email: wanglin@ict.ac.cn
#     HomePage: http://wangyoang.github.io
#      Version: 0.0.1
#   LastChange: 2014-10-13 14:55:53
#      History:
============================================================================= */

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition


// Load the graph from the edge list file
val graph = GraphLoader.edgeListFile(sc, "karate.links")

// Compute the pagerank of the graph
val ranks = graph.pageRank(0.0001).vertices
println(ranks.collect.mkString("\n"))


// Compute the principle singular value of the adjacent matrix of the graph
val mat: RowMatrix = ...
val svd: SinglularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(1, computeU = false)
val s: Vector = svd.s
