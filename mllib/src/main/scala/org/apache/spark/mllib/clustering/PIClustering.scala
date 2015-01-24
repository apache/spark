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

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

object PIClustering {

  private val logger = Logger.getLogger(getClass.getName())
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
  val RDDLA = RDDLinalg

  /**
   *
   * @param sc
   * @param points
   * @param nClusters
   * @param nIterations
   * @param sigma
   * @param minAffinity
   * @return Tuple of (Seq[(Cluster Id,Cluster Center)],
   *                   Seq[(VertexId, ClusterID Membership)]
   */
  def run(sc: SparkContext,
          points: Points,
          nClusters: Int,
          nIterations: Int = DefaultIterations,
          sigma: Double = DefaultSigma,
          minAffinity: Double = DefaultMinAffinity)
        : (Seq[(Int, Vector)], Seq[(VertexId, Int)]) = {
    val vidsRdd = sc.parallelize(points.map(_._1).sorted)
    val nVertices = points.length

    val (wRdd, rowSums) = createNormalizedAffinityMatrix(sc, points, sigma)
    val initialVt = createInitialVector(sc, points.map(_._1), rowSums)
    if (logger.isDebugEnabled) {
      logger.debug(s"Vt(0)=${
        LA.printVector(initialVt.map {
          _._2
        }.toArray)
      }")
    }
    val edgesRdd = createSparseEdgesRdd(sc, wRdd, minAffinity)
    val G = createGraphFromEdges(sc, edgesRdd, points.size, Some(initialVt))
    if (logger.isDebugEnabled) {
      logger.debug(RDDLA.printMatrixFromEdges(G.edges))
    }
    val (gUpdated, lambda, vt) = getPrincipalEigen(sc, G, nIterations)
    // TODO: avoid local collect and then sc.parallelize.
    val localVt = vt.collect.sortBy(_._1).map(_._2)
    val vectRdd = sc.parallelize(localVt.map(Vectors.dense(_)))
    // TODO: what to set nRuns
    val nRuns = 10
    vectRdd.cache()
    val model = KMeans.train(vectRdd, nClusters, nRuns)
    vectRdd.unpersist()
    if (logger.isDebugEnabled) {
      logger.debug(s"Eigenvalue = $lambda EigenVector: ${localVt.mkString(",")}")
    }
    val estimates = vidsRdd.zip(model.predict(sc.parallelize(localVt.map {
      Vectors.dense(_)
    })))
    if (logger.isDebugEnabled) {
      logger.debug(s"lambda=$lambda  eigen=${localVt.mkString(",")}")
    }
    val ccs = (0 until model.clusterCenters.length).zip(model.clusterCenters)
    if (logger.isDebugEnabled) {
      logger.debug(s"Kmeans model cluster centers: ${ccs.mkString(",")}")
    }
    val pointsMap = Map(points: _*)
    val estCollected = estimates.collect.sortBy(_._1)
    if (logger.isDebugEnabled) {
      //      val clusters = estCollected.map(_._2)
      //      logger.debug(s"Cluster Estimates: ${estCollected.mkString(",")} "
      //      val counts = Map(estCollected:_*).groupBy(_._1).mapValues(_.size)
      //        + s" Counts: ${counts.mkString(",")}")
      logger.debug(s"Cluster Estimates: ${estCollected.mkString(",")}")
    }
    (ccs, estCollected)
  }

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

  def getPrincipalEigen(sc: SparkContext,
                        G: DGraph,
                        nIterations: Int = DefaultIterations,
                        optMinNormChange: Option[Double] = None
                         ): (DGraph, Double, VertexRDD[Double]) = {

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
      if (logger.isDebugEnabled) {
        logger.debug(s"tmpEigen[$iter]: ${tmpEigen.collect.mkString(",")}\n")
      }
      val vnorm =
        prevG.vertices.map {
          _._2
        }.fold(0.0) { case (sum, dval) =>
          sum + Math.abs(dval)
        }
      if (logger.isDebugEnabled) {
        logger.debug(s"vnorm[$iter]=$vnorm")
      }
      outG = prevG.outerJoinVertices(tmpEigen) { case (vid, wval, optTmpEigJ) =>
        val normedEig = optTmpEigJ.getOrElse {
          -1.0
        } / vnorm
        if (logger.isDebugEnabled) {
          logger.debug(s"Updating vertex[$vid] from $wval to $normedEig")
        }
        normedEig
      }
      prevG = outG

      if (logger.isDebugEnabled) {
        val localVertices = outG.vertices.collect
        val graphSize = localVertices.size
        print(s"Vertices[$iter]: ${localVertices.mkString(",")}\n")
      }
      normVelocity = vnorm - priorNorm
      normAccel = normVelocity - priorNormVelocity
      if (logger.isDebugEnabled) {
        logger.debug(s"normAccel[$iter]= $normAccel")
      }
      priorNorm = vnorm
      priorNormVelocity = vnorm - priorNorm
    }
    (outG, vnorm, outG.vertices)
  }

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
    if (logger.isDebugEnabled) {
      logger.debug(s"Read in ${vertices.length} from $verticesFile")
    }
    vertices
  }

  def gaussianDist(c1arr: DVector, c2arr: DVector, sigma: Double) = {
    val c1c2 = c1arr.zip(c2arr)
    val dist = Math.exp((-0.5 / Math.pow(sigma, 2.0)) * c1c2.foldLeft(0.0) {
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
    val affinityRddNotNorm = sc.parallelize({
      val ivect = new Array[IndexedVector](nVertices)
      for (i <- 0 until points.size) {
        ivect(i) = new IndexedVector(points(i)._1, new DVector(nVertices))
        for (j <- 0 until points.size) {
          val dist = if (i != j) {
            gaussianDist(points(i)._2, points(j)._2, sigma)
          } else {
            0.0
          }
          ivect(i)._2(j) = dist
        }
      }
      ivect.zipWithIndex.map { case (vect, ix) =>
        (ix, vect)
      }
    }, nVertices)
    if (logger.isDebugEnabled) {
      logger.debug(s"Affinity:\n${
        LA.printMatrix(affinityRddNotNorm.collect.map(_._2._2),
          nVertices, nVertices)
      }")
    }
    val rowSums = affinityRddNotNorm.map { case (ix, (vid, vect)) =>
      vect.foldLeft(0.0) {
        _ + _
      }
    }
    val materializedRowSums = rowSums.collect
    val similarityRdd = affinityRddNotNorm.map { case (rowx, (vid, vect)) =>
      (vid, vect.map {
        _ / materializedRowSums(rowx)
      })
    }
    if (logger.isDebugEnabled) {
      logger.debug(s"W:\n${LA.printMatrix(similarityRdd.collect.map(_._2), nVertices, nVertices)}")
    }
    (similarityRdd, materializedRowSums)
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
