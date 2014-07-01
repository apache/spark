package org.apache.spark.mllib.clustering

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{DenseVector => BDV, Vector => BV, norm => breezeNorm}

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom

trait KMeansCommons {
    /**
   * Initialize cluster centers for one run at random.
   */
  protected def initRandom(data: RDD[BreezeVectorWithNorm], k: Int)
  : Array[BreezeVectorWithNorm] = {
    // Sample all the cluster centers in one pass to avoid repeated scans
    val sample = data.takeSample(true, k, new XORShiftRandom().nextInt()).toSeq
    sample.map { v =>
      new BreezeVectorWithNorm(v.vector.toDenseVector, v.norm)
    }.toArray
  }

  /**
   * Initialize cluster centers for one run using the k-means|| algorithm by Bahmani et al.
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). This is a variant of k-means++ that tries
   * to find with dissimilar cluster centers by starting with a random center and then doing
   * passes where more centers are chosen with probability proportional to their squared distance
   * to the current cluster set. It results in a provable approximation to an optimal clustering.
   *
   * The original paper can be found at http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf.
   */
  protected def initParallel(data: RDD[BreezeVectorWithNorm],
      k: Int,
      initializationSteps: Int)
  : Array[BreezeVectorWithNorm] = {
    // Initialize each run's center to a random point
    val seed = new XORShiftRandom().nextInt()
    val sample = data.takeSample(true, 1, seed).toSeq
    val centers = ArrayBuffer() ++ sample

    // On each step, sample 2 * k points on average for each run with probability proportional
    // to their squared distance from that run's current centers
    var step = 0
    while (step < initializationSteps) {
      val sumCosts = data.map { point =>
      		KMeansMiniBatch.pointCost(centers, point)
      }.reduce(_ + _)
      val chosen = data.mapPartitionsWithIndex { (index, points) =>
        val rand = new XORShiftRandom(seed ^ (step << 16) ^ index)
        
        // accept / reject each point
        val sampledCenters = points.filter { p =>
          rand.nextDouble() < 2.0 * KMeansMiniBatch.pointCost(centers, p) * k / sumCosts
        }
        
        sampledCenters
      }.collect()
      
      
      centers ++= chosen
      step += 1
    }
    
    // Finally, we might have a set of more than k candidate centers for each run; weigh each
    // candidate by the number of points in the dataset mapping to it and run a local k-means++
    // on the weighted centers to pick just k of them
    val weightMap = data.map { p =>
        (KMeansMiniBatch.findClosest(centers, p)._1, 1.0)
      }.reduceByKey(_ + _).collectAsMap()
    val weights = (0 until centers.length).map(i => weightMap.getOrElse(i, 0.0)).toArray
    val finalCenters = LocalKMeans.kMeansPlusPlus(seed, centers.toArray, weights, k, 30)

    finalCenters.toArray
  }
}

trait KMeansObjectCommons {
  
    /**
   * Returns the index of the closest center to the given point, as well as the squared distance.
   */
  private[mllib] def findClosest(
      centers: TraversableOnce[BreezeVectorWithNorm],
      point: BreezeVectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
      // distance computation.
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }

  /**
   * Returns the K-means cost of a given point against the given cluster centers.
   */
  private[mllib] def pointCost(
      centers: TraversableOnce[BreezeVectorWithNorm],
      point: BreezeVectorWithNorm): Double =
    findClosest(centers, point)._2

  /**
   * Returns the squared Euclidean distance between two vectors computed by
   * [[org.apache.spark.mllib.util.MLUtils#fastSquaredDistance]].
   */
  private[clustering] def fastSquaredDistance(
      v1: BreezeVectorWithNorm,
      v2: BreezeVectorWithNorm): Double = {
    MLUtils.fastSquaredDistance(v1.vector, v1.norm, v2.vector, v2.norm)
  }
  
}

/**
 * A breeze vector with its norm for fast distance computation.
 *
 * @see [[org.apache.spark.mllib.clustering.KMeans#fastSquaredDistance]]
 */
private[clustering]
class BreezeVectorWithNorm(val vector: BV[Double], val norm: Double) extends Serializable {

  def this(vector: BV[Double]) = this(vector, breezeNorm(vector, 2.0))

  def this(array: Array[Double]) = this(new BDV[Double](array))

  def this(v: Vector) = this(v.toBreeze)

  /** Converts the vector to a dense vector. */
  def toDense = new BreezeVectorWithNorm(vector.toDenseVector, norm)
}