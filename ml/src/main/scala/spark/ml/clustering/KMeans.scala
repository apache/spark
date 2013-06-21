package spark.ml.clustering

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import spark.{SparkContext, RDD}
import spark.SparkContext._
import spark.Logging

import org.jblas.DoubleMatrix


/**
 * K-means clustering with support for multiple parallel runs and a k-means++ like initialization
 * mode (the k-means|| algorithm by TODO). When multiple concurrent runs are requested, they are
 * executed together with joint passes over the data for efficiency.
 *
 * This is an iterative algorithm that will make multiple passes over the data, so any RDDs given
 * to it should be cached by the user.
 */
class KMeans private (
    var k: Int,
    var maxIterations: Int,
    var runs: Int,
    var initializationMode: String,
    var initializationSteps: Int)
  extends Serializable with Logging
{
  // Initialization mode names
  private val RANDOM = "random"
  private val K_MEANS_PARALLEL = "k-means||"

  def this() = this(2, 20, 1, "k-means||", 5)

  /** Set the number of clusters to create (k). Default: 2. */
  def setK(k: Int): KMeans = {
    this.k = k
    this
  }

  /** Set maximum number of iterations to run. Default: 20. */
  def setMaxIterations(maxIterations: Int): KMeans = {
    this.maxIterations = maxIterations
    this
  }

  /**
   * Set the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "k-means||" to use a parallel version of k-means++ discussed
   * in (TODO). Default: k-means||.
   */
  def setInitializationMode(initializationMode: String): KMeans = {
    if (initializationMode != RANDOM && initializationMode != K_MEANS_PARALLEL) {
      throw new IllegalArgumentException("Invalid initialization mode: " + initializationMode)
    }
    this.initializationMode = initializationMode
    this
  }

  /**
   * Set the number of runs of the algorithm to execute in parallel. We initialize the algorithm
   * this many times with random starting conditions (configured by the initialization mode), then
   * return the best clustering found over any run. Default: 1.
   */
  def setRuns(runs: Int): KMeans = {
    this.runs = runs
    this
  }

  /**
   * Set the number of steps for the k-means|| initialization mode. This is an advanced
   * setting -- the default of 5 is almost always enough. Default: 5.
   */
  def setInitializationSteps(initializationSteps: Int): KMeans = {
    this.initializationSteps = initializationSteps
    this
  }

  /**
   * Train a K-means model on the given set of points; `data` should be cached for high
   * performance, because this is an iterative algorithm.
   */
  def train(data: RDD[Array[Double]]): KMeansModel = {
    // TODO: check whether data is persistent; this needs RDD.storageLevel to be publicly readable

    null
  }
}


/**
 * Top-level methods for calling K-means clustering.
 */
object KMeans {
  def train(
      data: RDD[Array[Double]],
      k: Int,
      maxIterations: Int,
      runs: Int,
      initializationMode: String)
    : KMeansModel =
  {
    new KMeans().setK(k)
                .setMaxIterations(maxIterations)
                .setRuns(runs)
                .setInitializationMode(initializationMode)
                .train(data)
  }
  
  def train(data: RDD[Array[Double]], k: Int, maxIterations: Int, runs: Int): KMeansModel = {
    train(data, k, maxIterations, runs, "k-means||")
  }
  
  def train(data: RDD[Array[Double]], k: Int, maxIterations: Int): KMeansModel = {
    train(data, k, maxIterations, 1, "k-means||")
  }
}
