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

import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

/**
 * :: DeveloperApi ::
 * StreamingKMeansModel extends MLlib's KMeansModel for streaming
 * algorithms, so it can keep track of a continuously updated weight
 * associated with each cluster, and also update the model by
 * doing a single iteration of the standard k-means algorithm.
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
    val clusterWeights: Array[Double]) extends KMeansModel(clusterCenters) with Logging {

  /** Perform a k-means update on a batch of data. */
  def update(data: RDD[Vector], decayFactor: Double, timeUnit: String): StreamingKMeansModel = {

    // find nearest cluster to each point
    val closest = data.map(point => (this.predict(point), (point, 1L)))

    // get sums and counts for updating each cluster
    val mergeContribs: ((Vector, Long), (Vector, Long)) => (Vector, Long) = (p1, p2) => {
      BLAS.axpy(1.0, p2._1, p1._1)
      (p1._1, p1._2 + p2._2)
    }
    val dim = clusterCenters(0).size
    val pointStats: Array[(Int, (Vector, Long))] = closest
      .aggregateByKey((Vectors.zeros(dim), 0L))(mergeContribs, mergeContribs)
      .collect()

    val discount = timeUnit match {
      case StreamingKMeans.BATCHES => decayFactor
      case StreamingKMeans.POINTS =>
        val numNewPoints = pointStats.view.map { case (_, (_, n)) =>
          n
        }.sum
        math.pow(decayFactor, numNewPoints)
    }

    // apply discount to weights
    BLAS.scal(discount, Vectors.dense(clusterWeights))

    // implement update rule
    pointStats.foreach { case (label, (sum, count)) =>
      val centroid = clusterCenters(label)

      val updatedWeight = clusterWeights(label) + count
      val lambda = count / math.max(updatedWeight, 1e-16)

      clusterWeights(label) = updatedWeight
      BLAS.scal(1.0 - lambda, centroid)
      BLAS.axpy(lambda / count, sum, centroid)

      // display the updated cluster centers
      val display = clusterCenters(label).size match {
        case x if x > 100 => centroid.toArray.take(100).mkString("[", ",", "...")
        case _ => centroid.toArray.mkString("[", ",", "]")
      }

      logInfo(s"Cluster $label updated with weight $updatedWeight and centroid: $display")
    }

    // Check whether the smallest cluster is dying. If so, split the largest cluster.
    val weightsWithIndex = clusterWeights.view.zipWithIndex
    val (maxWeight, largest) = weightsWithIndex.maxBy(_._1)
    val (minWeight, smallest) = weightsWithIndex.minBy(_._1)
    if (minWeight < 1e-8 * maxWeight) {
      logInfo(s"Cluster $smallest is dying. Split the largest cluster $largest into two.")
      val weight = (maxWeight + minWeight) / 2.0
      clusterWeights(largest) = weight
      clusterWeights(smallest) = weight
      val largestClusterCenter = clusterCenters(largest)
      val smallestClusterCenter = clusterCenters(smallest)
      var j = 0
      while (j < dim) {
        val x = largestClusterCenter(j)
        val p = 1e-14 * math.max(math.abs(x), 1.0)
        largestClusterCenter.toBreeze(j) = x + p
        smallestClusterCenter.toBreeze(j) = x - p
        j += 1
      }
    }

    this
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
 *    .setRandomCenters(5, 100.0)
 *    .trainOn(DStream)
 */
@DeveloperApi
class StreamingKMeans(
    var k: Int,
    var decayFactor: Double,
    var timeUnit: String) extends Logging {

  def this() = this(2, 1.0, StreamingKMeans.BATCHES)

  protected var model: StreamingKMeansModel = new StreamingKMeansModel(null, null)

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
    if (timeUnit != StreamingKMeans.BATCHES && timeUnit != StreamingKMeans.POINTS) {
      throw new IllegalArgumentException("Invalid time unit for decay: " + timeUnit)
    }
    this.decayFactor = math.exp(math.log(0.5) / halfLife)
    logInfo("Setting decay factor to: %g ".format (this.decayFactor))
    this.timeUnit = timeUnit
    this
  }

  /** Specify initial centers directly. */
  def setInitialCenters(centers: Array[Vector], weights: Array[Double]): this.type = {
    model = new StreamingKMeansModel(centers, weights)
    this
  }

  /**
   * Initialize random centers, requiring only the number of dimensions.
   *
   * @param dim Number of dimensions
   * @param weight Weight for each center
   * @param seed Random seed
   */
  def setRandomCenters(dim: Int, weight: Double, seed: Long = Utils.random.nextLong): this.type = {
    val random = new XORShiftRandom(seed)
    val centers = Array.fill(k)(Vectors.dense(Array.fill(dim)(random.nextGaussian())))
    val weights = Array.fill(k)(weight)
    model = new StreamingKMeansModel(centers, weights)
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
    assertInitialized()
    data.foreachRDD { (rdd, time) =>
      model = model.update(rdd, decayFactor, timeUnit)
    }
  }

  /**
   * Use the clustering model to make predictions on batches of data from a DStream.
   *
   * @param data DStream containing vector data
   * @return DStream containing predictions
   */
  def predictOn(data: DStream[Vector]): DStream[Int] = {
    assertInitialized()
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
    assertInitialized()
    data.mapValues(model.predict)
  }

  /** Check whether cluster centers have been initialized. */
  private[this] def assertInitialized(): Unit = {
    if (model.clusterCenters == null) {
      throw new IllegalStateException(
        "Initial cluster centers must be set before starting predictions")
    }
  }
}

private[clustering] object StreamingKMeans {
  final val BATCHES = "batches"
  final val POINTS = "points"
}
