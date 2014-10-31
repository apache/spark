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

import scala.reflect.ClassTag

import breeze.linalg.{Vector => BV}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * StreamingKMeansModel extends MLlib's KMeansModel for streaming
 * algorithms, so it can keep track of the number of points assigned
 * to each cluster, and also update the model by doing a single iteration
 * of the standard k-means algorithm.
 *
 * The update algorithm uses the "mini-batch" KMeans rule,
 * generalized to incorporate forgetfullness (i.e. decay).
 * The update rule (for each cluster) is:
 *
 * c_t+1 = [(c_t * n_t * a) + (x_t * m_t)] / [n_t + m_t]
 * n_t+t = n_t * a + m_t
 *
 * Where c_t is the previously estimated centroid for that cluster,
 * n_t is the number of points assigned to it thus far, x_t is the centroid
 * estimated on the current batch, and m_t is the number of points assigned
 * to that centroid in the current batch.
 *
 * The decay factor 'a' scales the contribution of the clusters as estimated thus far,
 * by applying a as a discount weighting on the current point when evaluating
 * new incoming data. If a=1, all batches are weighted equally. If a=0, new centroids
 * are determined entirely by recent data. Lower values correspond to
 * more forgetting.
 *
 * Decay can optionally be specified by a half life and associated
 * time unit. The time unit can either be a batch of data or a single
 * data point. Considering data arrived at time t, the half life h is defined
 * such that at time t + h the discount applied to the data from t is 0.5.
 * The definition remains the same whether the time unit is given
 * as batches or points.
 *
 */
@DeveloperApi
class StreamingKMeansModel(
    override val clusterCenters: Array[Vector],
    val clusterCounts: Array[Long]) extends KMeansModel(clusterCenters) with Logging {

  /** Perform a k-means update on a batch of data. */
  def update(data: RDD[Vector], decayFactor: Double, timeUnit: String): StreamingKMeansModel = {

    val centers = clusterCenters
    val counts = clusterCounts

    // find nearest cluster to each point
    val closest = data.map(point => (this.predict(point), (point.toBreeze, 1.toLong)))

    // get sums and counts for updating each cluster
    type WeightedPoint = (BV[Double], Long)
    def mergeContribs(p1: WeightedPoint, p2: WeightedPoint): WeightedPoint = {
      (p1._1 += p2._1, p1._2 + p2._2)
    }
    val pointStats: Array[(Int, (BV[Double], Long))] =
      closest.reduceByKey(mergeContribs).collect()

    // implement update rule
    pointStats.foreach { case (label, (mean, count)) =>
      // store old count and centroid
      val oldCount = counts(label)
      val oldCentroid = centers(label).toBreeze
      // get new count and centroid
      val newCount = count
      val newCentroid = mean / newCount.toDouble
      // compute the normalized scale factor that controls forgetting
      val lambda = timeUnit match {
        case "batches" =>  newCount / (decayFactor * oldCount + newCount)
        case "points" => newCount / (math.pow(decayFactor, newCount) * oldCount + newCount)
      }
      // perform the update
      val updatedCentroid = oldCentroid + (newCentroid - oldCentroid) * lambda
      // store the new counts and centers
      counts(label) = oldCount + newCount
      centers(label) = Vectors.fromBreeze(updatedCentroid)

      // display the updated cluster centers
      val display = centers(label).size match {
        case x if x > 100 => centers(label).toArray.take(100).mkString("[", ",", "...")
        case _ => centers(label).toArray.mkString("[", ",", "]")
      }
      logInfo("Cluster %d updated: %s ".format (label, display))
    }
    new StreamingKMeansModel(centers, counts)
  }

}
/**
 * :: DeveloperApi ::
 * StreamingKMeans provides methods for configuring a
 * streaming k-means analysis, training the model on streaming,
 * and using the model to make predictions on streaming data.
 * See KMeansModel for details on algorithm and update rules.
 *
 * Use a builder pattern to construct a streaming k-means analysis
 * in an application, like:
 *
 *  val model = new StreamingKMeans()
 *    .setDecayFactor(0.5)
 *    .setK(3)
 *    .setRandomCenters(5)
 *    .trainOn(DStream)
 */
@DeveloperApi
class StreamingKMeans(
    var k: Int,
    var decayFactor: Double,
    var timeUnit: String) extends Logging {

  protected var model: StreamingKMeansModel = new StreamingKMeansModel(null, null)

  def this() = this(2, 1.0, "batches")

  /** Set the number of clusters. */
  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  /** Set the decay factor directly (for forgetful algorithms). */
  def setDecayFactor(a: Double): this.type = {
    this.decayFactor = decayFactor
    this
  }

  /** Set the half life and time unit ("batches" or "points") for forgetful algorithms. */
  def setHalfLife(halfLife: Double, timeUnit: String): this.type = {
    if (timeUnit != "batches" && timeUnit != "points") {
      throw new IllegalArgumentException("Invalid time unit for decay: " + timeUnit)
    }
    this.decayFactor = math.exp(math.log(0.5) / halfLife)
    logInfo("Setting decay factor to: %g ".format (this.decayFactor))
    this.timeUnit = timeUnit
    this
  }

  /** Specify initial centers directly. */
  def setInitialCenters(initialCenters: Array[Vector]): this.type = {
    val clusterCounts = new Array[Long](this.k)
    this.model = new StreamingKMeansModel(initialCenters, clusterCounts)
    this
  }

  /** Initialize random centers, requiring only the number of dimensions.
    *
    * @param dim Number of dimensions
    * @param seed Random seed
    * */
  def setRandomCenters(dim: Int, seed: Long = Utils.random.nextLong): this.type = {

    val random = Utils.random
    random.setSeed(seed)

    val initialCenters = (0 until k)
      .map(_ => Vectors.dense(Array.fill(dim)(random.nextGaussian()))).toArray
    val clusterCounts = new Array[Long](this.k)
    this.model = new StreamingKMeansModel(initialCenters, clusterCounts)
    this
  }

  /** Return the latest model. */
  def latestModel(): StreamingKMeansModel = {
    model
  }

  /**
   * Update the clustering model by training on batches of data from a DStream.
   * This operation registers a DStream for training the model,
   * checks whether the cluster centers have been initialized,
   * and updates the model using each batch of data from the stream.
   *
   * @param data DStream containing vector data
   */
  def trainOn(data: DStream[Vector]) {
    this.assertInitialized()
    data.foreachRDD { (rdd, time) =>
      model = model.update(rdd, this.decayFactor, this.timeUnit)
    }
  }

  /**
   * Use the clustering model to make predictions on batches of data from a DStream.
   *
   * @param data DStream containing vector data
   * @return DStream containing predictions
   */
  def predictOn(data: DStream[Vector]): DStream[Int] = {
    this.assertInitialized()
    data.map(model.predict)
  }

  /**
   * Use the model to make predictions on the values of a DStream and carry over its keys.
   *
   * @param data DStream containing (key, feature vector) pairs
   * @tparam K key type
   * @return DStream containing the input keys and the predictions as values
   */
  def predictOnValues[K: ClassTag](data: DStream[(K, Vector)]): DStream[(K, Int)] = {
    this.assertInitialized()
    data.mapValues(model.predict)
  }

  /** Check whether cluster centers have been initialized. */
  def assertInitialized(): Unit = {
    if (Option(model.clusterCenters) == None) {
      throw new IllegalStateException(
        "Initial cluster centers must be set before starting predictions")
    }
  }

}
