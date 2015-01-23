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

package org.apache.spark.mllib.clustering

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * SpectralClusteringWithGraphx
 *
 */
object PIClustering {

  type DVector = Array[Double]

  type DEdge = Edge[Double]
  type LabeledPoint = (VertexId, DVector)

  type Points = Seq[LabeledPoint]

  type DGraph = Graph[Double, Double]

  type IndexedVector = (Long, DVector)

  val DefaultMinNormChange: Double = 1e-11

  val DefaultSigma = 1.0
  val DefaultIterations: Int = 20
  val DefaultMinAffinity = 1e-11

  val LA = PICLinalg

  def cluster(sc: SparkContext,
              points: Points,
              nClusters: Int,
              nIterations: Int = DefaultIterations,
              sigma: Double = DefaultSigma,
              minAffinity: Double = DefaultMinAffinity) = {
    val nVertices = points.length

    val (wRdd, rowSums) = createNormalizedAffinityMatrix(sc, points, sigma)
    val initialVt = createInitialVector(sc, points.map(_._1), rowSums)
    val edgesRdd = createSparseEdgesRdd(sc, wRdd, minAffinity)

    val G = createGraphFromEdges(sc, edgesRdd, points.size, Some(initialVt))
    getPrincipalEigen(sc, G)
  }

  /*

vnorm[0]=2.019968019268192
Updating vertex[0] from 0.2592592592592593 to 0.2597973189724011
Updating vertex[1] from 0.19753086419753088 to 0.1695805301675885
Updating vertex[3] from 0.2654320987654321 to 0.27258531045499795
Updating vertex[2] from 0.2777777777777778 to 0.29803684040501227

   */
  def createInitialVector(sc: SparkContext,
                          labels: Seq[VertexId],
                          rowSums: Seq[Double]) = {
    val volume = rowSums.fold(0.0) {
      _ + _
    }
    val initialVt = labels.zip(rowSums.map(_ / volume))
    initialVt
  }

  def createGraphFromEdges(sc: SparkContext,
                           edgesRdd: RDD[DEdge],
                           nPoints: Int,
                           optInitialVt: Option[Seq[(VertexId, Double)]] = None) = {

    assert(nPoints > 0, "Must provide number of points from the original dataset")
    val G = if (optInitialVt.isDefined) {
      val initialVt = optInitialVt.get
      val vertsRdd = sc.parallelize(initialVt)
      Graph(vertsRdd, edgesRdd)
    } else {
      Graph.fromEdges(edgesRdd, -1.0)
    }
    G

  }

  val printMatrices = true

  def getPrincipalEigen(sc: SparkContext,
                        G: DGraph,
                        nIterations: Int = DefaultIterations,
                        optMinNormChange: Option[Double] = None
                         ): (DGraph, Double, DVector) = {

    var priorNorm = Double.MaxValue
    var norm = Double.MaxValue
    var priorNormVelocity = Double.MaxValue
    var normVelocity = Double.MaxValue
    var normAccel = Double.MaxValue
    val DummyVertexId = -99L
    var vnorm: Double = -1.0
    var outG: DGraph = null
    var prevG: DGraph = G
    val epsilon = optMinNormChange
      .getOrElse(1e-5 / G.vertices.count())
    for (iter <- 0 until nIterations
         if Math.abs(normAccel) > epsilon) {

      val tmpEigen = prevG.aggregateMessages[Double](ctx => {
        ctx.sendToSrc(ctx.attr * ctx.srcAttr);
        ctx.sendToDst(ctx.attr * ctx.dstAttr)
      },
        _ + _)
      println(s"tmpEigen[$iter]: ${tmpEigen.collect.mkString(",")}\n")
      val vnorm =
        prevG.vertices.map{ _._2}.fold(0.0) { case (sum, dval) =>
          sum + Math.abs(dval)
        }
      println(s"vnorm[$iter]=$vnorm")
      outG = prevG.outerJoinVertices(tmpEigen) { case (vid, wval, optTmpEigJ) =>
        val normedEig = optTmpEigJ.getOrElse {
          println("We got null estimated eigenvector element");
          -1.0
        } / vnorm
        println(s"Updating vertex[$vid] from $wval to $normedEig")
        normedEig
      }
      prevG = outG

      if (printMatrices) {
        val localVertices = outG.vertices.collect
        val graphSize = localVertices.size
        print(s"Vertices[$iter]: ${localVertices.mkString(",")}\n")
      }
      normVelocity = vnorm - priorNorm
      normAccel = normVelocity - priorNormVelocity
      println(s"normAccel[$iter]= $normAccel")
      priorNorm = vnorm
      priorNormVelocity = vnorm - priorNorm
    }
    (outG, vnorm, outG.vertices.collect.map {
      _._2
    })
  }

  //  def printGraph(G: DGraph) = {
  //    val collectedVerts = G.vertices.collect
  //    val nVertices = collectedVerts.length
  //      val msg = s"Graph Vertices:\n${printMatrix(collectedVerts, nVertices, nVertices)}"
  //  }
  //
  def scalarDot(d1: DVector, d2: DVector) = {
    Math.sqrt(d1.zip(d2).foldLeft(0.0) { case (sum, (d1v, d2v)) =>
      sum + d1v * d2v
    })
  }

  def vectorDot(d1: DVector, d2: DVector) = {
    d1.zip(d2).map { case (d1v, d2v) =>
      d1v * d2v
    }
  }

  def normVect(d1: DVector, d2: DVector) = {
    val scaldot = scalarDot(d1, d2)
    vectorDot(d1, d2).map {
      _ / scaldot
    }
  }

  def readVerticesfromFile(verticesFile: String): Points = {

    import scala.io.Source
    val vertices = Source.fromFile(verticesFile).getLines.map { l =>
      val toks = l.split("\t")
      val arr = toks.slice(1, toks.length).map(_.toDouble)
      (toks(0).toLong, arr)
    }.toSeq
    println(s"Read in ${vertices.length} from $verticesFile")
    //    println(vertices.map { case (x, arr) => s"($x,${arr.mkString(",")})"}
    // .mkString("[", ",\n", "]"))
    vertices
  }

  def gaussianDist(c1arr: DVector, c2arr: DVector, sigma: Double) = {
    val c1c2 = c1arr.zip(c2arr)
    val dist = Math.exp((0.5 / Math.pow(sigma, 2.0)) * c1c2.foldLeft(0.0) {
      case (dist: Double, (c1: Double, c2: Double)) =>
        dist + Math.pow(c1 - c2, 2)
    })
    dist
  }

  def createSparseEdgesRdd(sc: SparkContext, wRdd: RDD[IndexedVector],
                           minAffinity: Double = DefaultMinAffinity) = {
    val labels = wRdd.map { case (vid, vect) => vid}.collect
    val edgesRdd = wRdd.flatMap { case (vid, vect) =>
      for ((dval, ix) <- vect.zipWithIndex
           if Math.abs(dval) >= minAffinity)
      yield Edge(vid, labels(ix), dval)
    }
    edgesRdd
  }

  def createNormalizedAffinityMatrix(sc: SparkContext, points: Points, sigma: Double) = {
    val nVertices = points.length
    val rowSums = for (bcx <- 0 until nVertices)
    yield sc.accumulator[Double](bcx, s"ColCounts$bcx")
    val affinityRddNotNorm = sc.parallelize({
      val ivect = new Array[IndexedVector](nVertices)
      var rsum = 0.0
      for (i <- 0 until points.size) {
        ivect(i) = new IndexedVector(points(i)._1, new DVector(nVertices))
        for (j <- 0 until points.size) {
          val dist = if (i != j) {
            gaussianDist(points(i)._2, points(j)._2, sigma)
          } else {
            0.0
          }
          ivect(i)._2(j) = dist
          rsum += dist
        }
        rowSums(i) += rsum
      }
      ivect.zipWithIndex.map { case (vect, ix) =>
        (ix, vect)
      }
    }, nVertices)
    val affinityRdd = affinityRddNotNorm.map { case (rowx, (vid, vect)) =>
      (vid, vect.map {
        _ / rowSums(rowx).value
      })
    }
    (affinityRdd, rowSums.map {
      _.value
    })
  }

  def norm(vect: DVector): Double = {
    Math.sqrt(vect.foldLeft(0.0) { case (sum, dval) => sum + Math.pow(dval, 2)})
  }

  def printMatrix(darr: Array[DVector], numRows: Int, numCols: Int): String = {
    val flattenedArr = darr.zipWithIndex.foldLeft(new DVector(numRows * numCols)) {
      case (flatarr, (row, indx)) =>
        System.arraycopy(row, 0, flatarr, indx * numCols, numCols)
        flatarr
    }
    printMatrix(flattenedArr, numRows, numCols)
  }

  def printMatrix(darr: DVector, numRows: Int, numCols: Int): String = {
    val stride = (darr.length / numCols)
    val sb = new StringBuilder
    def leftJust(s: String, len: Int) = {
      "         ".substring(0, len - Math.min(len, s.length)) + s
    }

    for (r <- 0 until numRows) {
      for (c <- 0 until numCols) {
        sb.append(leftJust(f"${darr(c * stride + r)}%.6f", 9) + " ")
      }
      sb.append("\n")
    }
    sb.toString
  }

  def printVect(dvect: DVector) = {
    dvect.mkString(",")
  }

}
