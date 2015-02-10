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

package org.apache.spark.examples.mllib

import org.apache.log4j.{Level, Logger}
import scopt.OptionParser

import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * An example Power Iteration Clustering app.  Takes an input of K concentric circles
 * with a total of "n" sampled points (total here means "across ALL of the circles").
 * The output should be K clusters - each cluster containing precisely the points associated
 * with each of the input circles.
 *
 * Run with
 * {{{
 * ./bin/run-example mllib.PowerIterationClusteringExample [options]
 *
 * Where options include:
 *   k:  Number of circles/ clusters
 *   n:  Number of sampled points on innermost circle.. There are proportionally more points
 *      within the outer/larger circles
 *   numIterations:   Number of Power Iterations
 *   outerRadius:  radius of the outermost of the concentric circles
 * }}}
 *
 * Here is a sample run and output:
 *
 * ./bin/run-example mllib.PowerIterationClusteringExample
 * -k 3 --n 30 --numIterations 15
 *
 * Cluster assignments: 1 -> [0,1,2,3,4],2 -> [5,6,7,8,9,10,11,12,13,14],
 * 0 -> [15,16,17,18,19,20,21,22,23,24,25,26,27,28,29]
 *
 *
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object PowerIterationClusteringExample {

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("PIC Circles") {
      head("PowerIterationClusteringExample: an example PIC app using concentric circles.")
      opt[Int]('k', "k")
          .text(s"number of circles (/clusters), default: ${defaultParams.k}")
          .action((x, c) => c.copy(k = x))
      opt[Int]('n', "n")
          .text(s"number of points, default: ${defaultParams.numPoints}")
          .action((x, c) => c.copy(numPoints = x))
      opt[Int]("numIterations")
          .text(s"number of iterations, default: ${defaultParams.numIterations}")
          .action((x, c) => c.copy(numIterations = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf()
        .setMaster("local")
        .setAppName(s"PowerIterationClustering with $params")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val circlesRdd = generateCirclesRdd(sc, params.k, params.numPoints, params.outerRadius)
    val model = new PowerIterationClustering()
        .setK(params.k)
        .setMaxIterations(params.numIterations)
        .run(circlesRdd)

    val clusters = model.assignments.collect.groupBy(_._2).mapValues(_.map(_._1))
    val assignments = clusters.toList.sortBy { case (k, v) => v.length}
    val assignmentsStr = assignments
        .map { case (k, v) => s"$k -> ${v.sorted.mkString("[", ",", "]")}"}.mkString(",")
    println(s"Cluster assignments: $assignmentsStr")

    sc.stop()
  }

  def generateCirclesRdd(sc: SparkContext,
      nCircles: Int = 3,
      nPoints: Int = 30,
      outerRadius: Double): RDD[(Long, Long, Double)] = {

    val radii = for (cx <- 0 until nCircles) yield {
      cx match {
        case 0 => 0.1 * outerRadius / nCircles
        case _ if cx == nCircles - 1 => outerRadius
        case _ => outerRadius * cx / (nCircles - 1)
      }
    }
    val groupSizes = for (cx <- 0 until nCircles) yield (cx+1) * nPoints
    var ix = 0
    val points = for (cx <- 0 until nCircles;
                      px <- 0 until groupSizes(cx)) yield {
      val theta = 2.0 * math.Pi * px / groupSizes(cx)
      val out = (ix, (radii(cx) * math.cos(theta), radii(cx) * math.sin(theta)))
      ix += 1
      out
    }
    val rdd = sc.parallelize(points)
    val distancesRdd = rdd.cartesian(rdd).flatMap { case ((i0, (x0, y0)), (i1, (x1, y1))) =>
      if (i0 < i1) {
        Some((i0.toLong, i1.toLong, similarity((x0, y0), (x1, y1))))
      } else {
        None
      }
    }
    distancesRdd
  }

  private[mllib] def similarity(p1: (Double, Double), p2: (Double, Double)) = {
    gaussianSimilarity(p1, p2, 1.0)
  }

  /**
   * Gaussian Similarity:  http://en.wikipedia.org/wiki/Radial_basis_function_kernel
   */
  def gaussianSimilarity(p1: (Double, Double), p2: (Double, Double), sigma: Double) = {
    math.exp((p1._1 - p2._1) * (p1._1 - p2._1) + (p1._2 - p2._2) * (p1._2 - p2._2))
  }

  case class Params(
      input: String = null,
      k: Int = 3,
      numPoints: Int = 5,
      numIterations: Int = 10,
      outerRadius: Double = 3.0
      ) extends AbstractParams[Params]

}
