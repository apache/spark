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

// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.clustering.PowerIterationClustering
// $example off$
import org.apache.spark.rdd.RDD

/**
 * An example Power Iteration Clustering app.
 * http://www.cs.cmu.edu/~frank/papers/icml2010-pic-final.pdf
 * Takes an input of K concentric circles and the number of points in the innermost circle.
 * The output should be K clusters - each cluster containing precisely the points associated
 * with each of the input circles.
 *
 * Run with
 * {{{
 * ./bin/run-example mllib.PowerIterationClusteringExample [options]
 *
 * Where options include:
 *   k:  Number of circles/clusters
 *   n:  Number of sampled points on innermost circle.. There are proportionally more points
 *      within the outer/larger circles
 *   maxIterations:   Number of Power Iterations
 * }}}
 *
 * Here is a sample run and output:
 *
 * ./bin/run-example mllib.PowerIterationClusteringExample -k 2 --n 10 --maxIterations 15
 *
 * Cluster assignments: 1 -> [0,1,2,3,4,5,6,7,8,9],
 *   0 -> [10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29]
 *
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object PowerIterationClusteringExample {

  case class Params(
      k: Int = 2,
      numPoints: Int = 10,
      maxIterations: Int = 15
    ) extends AbstractParams[Params]

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("PowerIterationClusteringExample") {
      head("PowerIterationClusteringExample: an example PIC app using concentric circles.")
      opt[Int]('k', "k")
        .text(s"number of circles (clusters), default: ${defaultParams.k}")
        .action((x, c) => c.copy(k = x))
      opt[Int]('n', "n")
        .text(s"number of points in smallest circle, default: ${defaultParams.numPoints}")
        .action((x, c) => c.copy(numPoints = x))
      opt[Int]("maxIterations")
        .text(s"number of iterations, default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(s"PowerIterationClustering with $params")
    val sc = new SparkContext(conf)

    Configurator.setRootLevel(Level.WARN)

    // $example on$
    val circlesRdd = generateCirclesRdd(sc, params.k, params.numPoints)
    val model = new PowerIterationClustering()
      .setK(params.k)
      .setMaxIterations(params.maxIterations)
      .setInitializationMode("degree")
      .run(circlesRdd)

    val clusters = model.assignments.collect().groupBy(_.cluster).transform((_, v) => v.map(_.id))
    val assignments = clusters.toList.sortBy { case (k, v) => v.length }
    val assignmentsStr = assignments
      .map { case (k, v) =>
        s"$k -> ${v.sorted.mkString("[", ",", "]")}"
      }.mkString(", ")
    val sizesStr = assignments.map {
      _._2.length
    }.sorted.mkString("(", ",", ")")
    println(s"Cluster assignments: $assignmentsStr\ncluster sizes: $sizesStr")
    // $example off$

    sc.stop()
  }

  def generateCircle(radius: Double, n: Int): Seq[(Double, Double)] = {
    Seq.tabulate(n) { i =>
      val theta = 2.0 * math.Pi * i / n
      (radius * math.cos(theta), radius * math.sin(theta))
    }
  }

  def generateCirclesRdd(
      sc: SparkContext,
      nCircles: Int,
      nPoints: Int): RDD[(Long, Long, Double)] = {
    val points = (1 to nCircles).flatMap { i =>
      generateCircle(i, i * nPoints)
    }.zipWithIndex
    val rdd = sc.parallelize(points)
    val distancesRdd = rdd.cartesian(rdd).flatMap { case (((x0, y0), i0), ((x1, y1), i1)) =>
      if (i0 < i1) {
        Some((i0.toLong, i1.toLong, gaussianSimilarity((x0, y0), (x1, y1))))
      } else {
        None
      }
    }
    distancesRdd
  }

  /**
   * Gaussian Similarity:  http://en.wikipedia.org/wiki/Radial_basis_function_kernel
   */
  def gaussianSimilarity(p1: (Double, Double), p2: (Double, Double)): Double = {
    val ssquares = (p1._1 - p2._1) * (p1._1 - p2._1) + (p1._2 - p2._2) * (p1._2 - p2._2)
    math.exp(-ssquares / 2.0)
  }
}
// scalastyle:on println
