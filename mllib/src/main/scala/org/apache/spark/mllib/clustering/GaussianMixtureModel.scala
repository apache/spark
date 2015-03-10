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
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.{Vector, Matrices}
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian
import org.apache.spark.mllib.util.{MLUtils, Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * :: Experimental ::
 *
 * Multivariate Gaussian Mixture Model (GMM) consisting of k Gaussians, where points 
 * are drawn from each Gaussian i=1..k with probability w(i); mu(i) and sigma(i) are 
 * the respective mean and covariance for each Gaussian distribution i=1..k. 
 * 
 * @param weight Weights for each Gaussian distribution in the mixture, where weight(i) is
 *               the weight for Gaussian i, and weight.sum == 1
 * @param mu Means for each Gaussian in the mixture, where mu(i) is the mean for Gaussian i
 * @param sigma Covariance maxtrix for each Gaussian in the mixture, where sigma(i) is the
 *              covariance matrix for Gaussian i
 */
@Experimental
class GaussianMixtureModel(
  val weights: Array[Double], 
  val gaussians: Array[MultivariateGaussian]) extends Serializable with Saveable{
  
  require(weights.length == gaussians.length, "Length of weight and Gaussian arrays must match")

  override protected def formatVersion = "1.0"

  override def save(sc: SparkContext, path: String) : Unit = {
    GaussianMixtureModel.SaveLoadV1_0.save(sc, path, weights, gaussians)
  }

  /** Number of gaussians in mixture */
  def k: Int = weights.length

  /** Maps given points to their cluster indices. */
  def predict(points: RDD[Vector]): RDD[Int] = {
    val responsibilityMatrix = predictSoft(points)
    responsibilityMatrix.map(r => r.indexOf(r.max))
  }
  
  /**
   * Given the input vectors, return the membership value of each vector
   * to all mixture components. 
   */
  def predictSoft(points: RDD[Vector]): RDD[Array[Double]] = {
    val sc = points.sparkContext
    val bcDists = sc.broadcast(gaussians)
    val bcWeights = sc.broadcast(weights)
    points.map { x => 
      computeSoftAssignments(x.toBreeze.toDenseVector, bcDists.value, bcWeights.value, k)
    }
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

@Experimental
object GaussianMixtureModel extends Loader[GaussianMixtureModel] {

  object SaveLoadV1_0 {

    case class Data(weights: Array[Double], mus: Array[Vector], sigmas: Array[Array[Double]])

    def formatVersionV1_0 = "1.0"

    def classNameV1_0 = "org.apache.spark.mllib.clustering.GaussianMixtureModel"

    def save(
        sc: SparkContext,
        path: String,
        weights: Array[Double],
        gaussians: Array[MultivariateGaussian]) : Unit = {

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      // DataFrame does not recognize MultiVariateGaussian or Matrix as of now.
      val mus = gaussians.map(i => i.mu)
      val sigmas = gaussians.map(i => i.sigma.toArray)

      // Create JSON metadata.
      val metadata = compact(render
        (("class" -> classNameV1_0) ~ ("version" -> formatVersionV1_0) ~ ("k" -> weights.length)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      // Create Parquet data.
      val data = Data(weights, mus, sigmas)
      val dataRDD: DataFrame = sc.parallelize(Seq(data), 1).toDF()
      dataRDD.saveAsParquetFile(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String) : GaussianMixtureModel = {
      val datapath = Loader.dataPath(path)
      val sqlContext = new SQLContext(sc)
      val dataRDD = sqlContext.parquetFile(datapath)
      val dataArray = dataRDD.select("weights", "mus", "sigmas").take(1)
      // Check schema explicitly since erasure makes it hard to use match-case for checking.
      Loader.checkSchema[Data](dataRDD.schema)
      val data = dataArray(0)
      val weights = data.getAs[Seq[Double]](0).toArray
      val mus = data.getAs[Seq[Vector]](1).toArray
      val sigmas = data.getAs[Seq[Seq[Double]]](2).toArray
      val numFeatures = mus(0).size

      val gaussians = mus.zip(sigmas) map {
        case (mu, sigma) => {
          val mat = Matrices.dense(numFeatures, numFeatures, sigma.toArray)
          new MultivariateGaussian(mu, mat)
        }
      }
      return new GaussianMixtureModel(weights, gaussians)
    }
  }

  override def load(sc: SparkContext, path: String) : GaussianMixtureModel = {
    val (loadedClassName, version, metadata) = Loader.loadMetadata(sc, path)
    implicit val formats = DefaultFormats
    val k = (metadata \ "k").extract[Int]
    val classNameV1_0 = SaveLoadV1_0.classNameV1_0
    (loadedClassName, version) match {
      case (classNameV1_0, "1.0") => {
        val model = SaveLoadV1_0.load(sc, path)
        require(model.weights.length == k,
          s"GaussianMixtureModel requires weights of length $k " +
          s"got weights of length $model.weights.length")
        require(model.gaussians.length == k,
          s"GaussianMixtureModel requires gaussians of length $k" +
          s"got gaussians of length $model.gaussians.length")
        model
      }
      case _ => throw new Exception(
        s"GaussianMixtureModel.load did not recognize model with (className, format version):" +
        s"($loadedClassName, $version).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)")
    }
  }
}
