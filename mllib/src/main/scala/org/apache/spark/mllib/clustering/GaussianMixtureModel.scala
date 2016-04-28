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

import breeze.linalg.{DenseVector => BreezeVector}
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian
import org.apache.spark.mllib.util.{Loader, MLUtils, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

/**
 * Multivariate Gaussian Mixture Model (GMM) consisting of k Gaussians, where points
 * are drawn from each Gaussian i=1..k with probability w(i); mu(i) and sigma(i) are
 * the respective mean and covariance for each Gaussian distribution i=1..k.
 *
 * @param weights Weights for each Gaussian distribution in the mixture, where weights(i) is
 *                the weight for Gaussian i, and weights.sum == 1
 * @param gaussians Array of MultivariateGaussian where gaussians(i) represents
 *                  the Multivariate Gaussian (Normal) Distribution for Gaussian i
 */
@Since("1.3.0")
class GaussianMixtureModel @Since("1.3.0") (
  @Since("1.3.0") val weights: Array[Double],
  @Since("1.3.0") val gaussians: Array[MultivariateGaussian]) extends Serializable with Saveable {

  require(weights.length == gaussians.length, "Length of weight and Gaussian arrays must match")

  override protected def formatVersion = "1.0"

  @Since("1.4.0")
  override def save(sc: SparkContext, path: String): Unit = {
    GaussianMixtureModel.SaveLoadV1_0.save(sc, path, weights, gaussians)
  }

  /**
   * Number of gaussians in mixture
   */
  @Since("1.3.0")
  def k: Int = weights.length

  /**
   * Maps given points to their cluster indices.
   */
  @Since("1.3.0")
  def predict(points: RDD[Vector]): RDD[Int] = {
    val responsibilityMatrix = predictSoft(points)
    responsibilityMatrix.map(r => r.indexOf(r.max))
  }

  /**
   * Maps given point to its cluster index.
   */
  @Since("1.5.0")
  def predict(point: Vector): Int = {
    val r = predictSoft(point)
    r.indexOf(r.max)
  }

  /**
   * Java-friendly version of [[predict()]]
   */
  @Since("1.4.0")
  def predict(points: JavaRDD[Vector]): JavaRDD[java.lang.Integer] =
    predict(points.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Integer]]

  /**
   * Given the input vectors, return the membership value of each vector
   * to all mixture components.
   */
  @Since("1.3.0")
  def predictSoft(points: RDD[Vector]): RDD[Array[Double]] = {
    val sc = points.sparkContext
    val bcDists = sc.broadcast(gaussians)
    val bcWeights = sc.broadcast(weights)
    points.map { x =>
      computeSoftAssignments(x.toBreeze.toDenseVector, bcDists.value, bcWeights.value, k)
    }
  }

  /**
   * Given the input vector, return the membership values to all mixture components.
   */
  @Since("1.4.0")
  def predictSoft(point: Vector): Array[Double] = {
    computeSoftAssignments(point.toBreeze.toDenseVector, gaussians, weights, k)
  }

  /**
   * Compute the partial assignments for each vector
   */
  private def computeSoftAssignments(
      pt: BreezeVector[Double],
      dists: Array[MultivariateGaussian],
      weights: Array[Double],
      k: Int): Array[Double] = {
    val p = weights.zip(dists).map {
      case (weight, dist) => MLUtils.EPSILON + weight * dist.pdf(pt)
    }
    val pSum = p.sum
    for (i <- 0 until k) {
      p(i) /= pSum
    }
    p
  }
}

@Since("1.4.0")
object GaussianMixtureModel extends Loader[GaussianMixtureModel] {

  private object SaveLoadV1_0 {

    case class Data(weight: Double, mu: Vector, sigma: Matrix)

    val formatVersionV1_0 = "1.0"

    val classNameV1_0 = "org.apache.spark.mllib.clustering.GaussianMixtureModel"

    def save(
        sc: SparkContext,
        path: String,
        weights: Array[Double],
        gaussians: Array[MultivariateGaussian]): Unit = {

      val sqlContext = SQLContext.getOrCreate(sc)
      import sqlContext.implicits._

      // Create JSON metadata.
      val metadata = compact(render
        (("class" -> classNameV1_0) ~ ("version" -> formatVersionV1_0) ~ ("k" -> weights.length)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      // Create Parquet data.
      val dataArray = Array.tabulate(weights.length) { i =>
        Data(weights(i), gaussians(i).mu, gaussians(i).sigma)
      }
      sc.parallelize(dataArray, 1).toDF().write.parquet(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String): GaussianMixtureModel = {
      val dataPath = Loader.dataPath(path)
      val sqlContext = SQLContext.getOrCreate(sc)
      val dataFrame = sqlContext.read.parquet(dataPath)
      // Check schema explicitly since erasure makes it hard to use match-case for checking.
      Loader.checkSchema[Data](dataFrame.schema)
      val dataArray = dataFrame.select("weight", "mu", "sigma").collect()

      val (weights, gaussians) = dataArray.map {
        case Row(weight: Double, mu: Vector, sigma: Matrix) =>
          (weight, new MultivariateGaussian(mu, sigma))
      }.unzip

      new GaussianMixtureModel(weights.toArray, gaussians.toArray)
    }
  }

  @Since("1.4.0")
  override def load(sc: SparkContext, path: String): GaussianMixtureModel = {
    val (loadedClassName, version, metadata) = Loader.loadMetadata(sc, path)
    implicit val formats = DefaultFormats
    val k = (metadata \ "k").extract[Int]
    val classNameV1_0 = SaveLoadV1_0.classNameV1_0
    (loadedClassName, version) match {
      case (classNameV1_0, "1.0") =>
        val model = SaveLoadV1_0.load(sc, path)
        require(model.weights.length == k,
          s"GaussianMixtureModel requires weights of length $k " +
          s"got weights of length ${model.weights.length}")
        require(model.gaussians.length == k,
          s"GaussianMixtureModel requires gaussians of length $k" +
          s"got gaussians of length ${model.gaussians.length}")
        model
      case _ => throw new Exception(
        s"GaussianMixtureModel.load did not recognize model with (className, format version):" +
        s"($loadedClassName, $version).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)")
    }
  }
}
