package org.apache.spark.mllib.clustering

import breeze.linalg.{Vector => BV}

import scala.reflect.ClassTag
import scala.util.Random._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._

@DeveloperApi
class StreamingKMeansModel(
    override val clusterCenters: Array[Vector],
    val clusterCounts: Array[Long]) extends KMeansModel(clusterCenters) {

  /** do a sequential KMeans update on a batch of data **/
  def update(data: RDD[Vector], a: Double, units: String): StreamingKMeansModel = {

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
      closest.reduceByKey{mergeContribs}.collectAsMap().toArray

    // implement update rule
    for (newP <- pointStats) {
      // store old count and centroid
      val oldCount = counts(newP._1)
      val oldCentroid = centers(newP._1).toBreeze
      // get new count and centroid
      val newCount = newP._2._2
      val newCentroid = newP._2._1 / newCount.toDouble
      // compute the normalized scale factor that controls forgetting
      val decayFactor = units match {
        case "batches" =>  newCount / (a * oldCount + newCount)
        case "points" => newCount / (math.pow(a, newCount) * oldCount + newCount)
      }
      // perform the update
      val updatedCentroid = oldCentroid + (newCentroid - oldCentroid) * decayFactor
      // store the new counts and centers
      counts(newP._1) = oldCount + newCount
      centers(newP._1) = Vectors.fromBreeze(updatedCentroid)
    }

    new StreamingKMeansModel(centers, counts)
  }

}

@DeveloperApi
class StreamingKMeans(
     var k: Int,
     var a: Double,
     var units: String) extends Logging {

  protected var model: StreamingKMeansModel = new StreamingKMeansModel(null, null)

  def this() = this(2, 1.0, "batches")

  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  def setDecayFactor(a: Double): this.type = {
    this.a = a
    this
  }

  def setUnits(units: String): this.type = {
    this.units = units
    this
  }

  def setDecayFractionBatches(q: Double): this.type = {
    this.a = math.log(1 - q) / math.log(0.5)
    this.units = "batches"
    this
  }

  def setDecayFractionPoints(q: Double, m: Double): this.type = {
    this.a = math.pow(math.log(1 - q) / math.log(0.5), 1/m)
    this.units = "points"
    this
  }

  def setInitialCenters(initialCenters: Array[Vector]): this.type = {
    val clusterCounts = Array.fill(this.k)(0).map(_.toLong)
    this.model = new StreamingKMeansModel(initialCenters, clusterCounts)
    this
  }

  def setRandomCenters(d: Int): this.type = {
    val initialCenters = (0 until k).map(_ => Vectors.dense(Array.fill(d)(nextGaussian()))).toArray
    val clusterCounts = Array.fill(0)(d).map(_.toLong)
    this.model = new StreamingKMeansModel(initialCenters, clusterCounts)
    this
  }

  def latestModel(): StreamingKMeansModel = {
    model
  }

  def trainOn(data: DStream[Vector]) {
    this.isInitialized
    data.foreachRDD { (rdd, time) =>
      model = model.update(rdd, this.a, this.units)
    }
  }

  def predictOn(data: DStream[Vector]): DStream[Int] = {
    this.isInitialized
    data.map(model.predict)
  }

  def predictOnValues[K: ClassTag](data: DStream[(K, Vector)]): DStream[(K, Int)] = {
    this.isInitialized
    data.mapValues(model.predict)
  }

  def isInitialized: Boolean = {
    if (Option(model.clusterCenters) == None) {
      logError("Initial cluster centers must be set before starting predictions")
      throw new IllegalArgumentException
    } else {
      true
    }
  }

}
