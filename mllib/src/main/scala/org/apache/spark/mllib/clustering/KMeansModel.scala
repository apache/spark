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

import scala.collection.JavaConverters._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.pmml.PMMLExportable
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
 * A clustering model for K-means. Each point belongs to the cluster with the closest center.
 */
@Since("0.8.0")
class KMeansModel (@Since("1.0.0") val clusterCenters: Array[Vector],
  @Since("2.4.0") val distanceMeasure: String,
  @Since("2.4.0") val trainingCost: Double,
  private[spark] val numIter: Int)
  extends Saveable with Serializable with PMMLExportable {

  @transient private lazy val distanceMeasureInstance: DistanceMeasure =
    DistanceMeasure.decodeFromString(distanceMeasure)

  @transient private lazy val clusterCentersWithNorm =
    if (clusterCenters == null) null else clusterCenters.map(new VectorWithNorm(_))

  // TODO: computation of statistics may take seconds, so save it to KMeansModel in training
  @transient private lazy val statistics = if (clusterCenters == null) {
    None
  } else {
    val k = clusterCenters.length
    val numFeatures = clusterCenters.head.size
    if (DistanceMeasure.shouldComputeStatistics(k) &&
        DistanceMeasure.shouldComputeStatisticsLocally(k, numFeatures)) {
      Some(distanceMeasureInstance.computeStatistics(clusterCentersWithNorm))
    } else {
      None
    }
  }

  @Since("2.4.0")
  private[spark] def this(clusterCenters: Array[Vector], distanceMeasure: String) =
    this(clusterCenters: Array[Vector], distanceMeasure, 0.0, -1)

  @Since("1.1.0")
  def this(clusterCenters: Array[Vector]) =
    this(clusterCenters: Array[Vector], DistanceMeasure.EUCLIDEAN, 0.0, -1)

  /**
   * A Java-friendly constructor that takes an Iterable of Vectors.
   */
  @Since("1.4.0")
  def this(centers: java.lang.Iterable[Vector]) = this(centers.asScala.toArray)

  /**
   * Total number of clusters.
   */
  @Since("0.8.0")
  def k: Int = clusterCentersWithNorm.length

  /**
   * Returns the cluster index that a given point belongs to.
   */
  @Since("0.8.0")
  def predict(point: Vector): Int = {
    distanceMeasureInstance.findClosest(clusterCentersWithNorm, statistics,
      new VectorWithNorm(point))._1
  }

  /**
   * Maps given points to their cluster indices.
   */
  @Since("1.0.0")
  def predict(points: RDD[Vector]): RDD[Int] = {
    val bcCentersWithNorm = points.context.broadcast(clusterCentersWithNorm)
    val bcStatistics = points.context.broadcast(statistics)
    points.map(p =>
      distanceMeasureInstance.findClosest(bcCentersWithNorm.value,
        bcStatistics.value, new VectorWithNorm(p))._1)
  }

  /**
   * Maps given points to their cluster indices.
   */
  @Since("1.0.0")
  def predict(points: JavaRDD[Vector]): JavaRDD[java.lang.Integer] =
    predict(points.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Integer]]

  /**
   * Return the K-means cost (sum of squared distances of points to their nearest center) for this
   * model on the given data.
   */
  @Since("0.8.0")
  def computeCost(data: RDD[Vector]): Double = {
    val bcCentersWithNorm = data.context.broadcast(clusterCentersWithNorm)
    val cost = data.map(p =>
      distanceMeasureInstance.pointCost(bcCentersWithNorm.value, new VectorWithNorm(p)))
      .sum()
    bcCentersWithNorm.destroy()
    cost
  }


  @Since("1.4.0")
  override def save(sc: SparkContext, path: String): Unit = {
    KMeansModel.SaveLoadV2_0.save(sc, this, path)
  }
}

@Since("1.4.0")
object KMeansModel extends Loader[KMeansModel] {

  @Since("1.4.0")
  override def load(sc: SparkContext, path: String): KMeansModel = {
    val (loadedClassName, version, metadata) = Loader.loadMetadata(sc, path)
    val classNameV1_0 = SaveLoadV1_0.thisClassName
    val classNameV2_0 = SaveLoadV2_0.thisClassName
    (loadedClassName, version) match {
      case (className, "1.0") if className == classNameV1_0 =>
        SaveLoadV1_0.load(sc, path)
      case (className, "2.0") if className == classNameV2_0 =>
        SaveLoadV2_0.load(sc, path)
      case _ => throw new Exception(
        s"KMeansModel.load did not recognize model with (className, format version):" +
          s"($loadedClassName, $version).  Supported:\n" +
          s"  ($classNameV1_0, 1.0)\n" +
          s"  ($classNameV2_0, 2.0)")
    }
  }

  private[KMeansModel] case class Cluster(id: Int, point: Vector)

  private[KMeansModel] object Cluster {
    def apply(r: Row): Cluster = {
      Cluster(r.getInt(0), r.getAs[Vector](1))
    }
  }

  private[clustering] object SaveLoadV1_0 {

    private val thisFormatVersion = "1.0"

    private[clustering]
    val thisClassName = "org.apache.spark.mllib.clustering.KMeansModel"

    def save(sc: SparkContext, model: KMeansModel, path: String): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~ ("k" -> model.k)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))
      val dataRDD = sc.parallelize(model.clusterCentersWithNorm.zipWithIndex).map { case (p, id) =>
        Cluster(id, p.vector)
      }
      spark.createDataFrame(dataRDD).write.parquet(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String): KMeansModel = {
      implicit val formats = DefaultFormats
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)
      val k = (metadata \ "k").extract[Int]
      val centroids = spark.read.parquet(Loader.dataPath(path))
      Loader.checkSchema[Cluster](centroids.schema)
      val localCentroids = centroids.rdd.map(Cluster.apply).collect()
      assert(k == localCentroids.length)
      new KMeansModel(localCentroids.sortBy(_.id).map(_.point))
    }
  }

  private[clustering] object SaveLoadV2_0 {

    private val thisFormatVersion = "2.0"

    private[clustering] val thisClassName = "org.apache.spark.mllib.clustering.KMeansModel"

    def save(sc: SparkContext, model: KMeansModel, path: String): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion)
         ~ ("k" -> model.k) ~ ("distanceMeasure" -> model.distanceMeasure)
         ~ ("trainingCost" -> model.trainingCost)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))
      val dataRDD = sc.parallelize(model.clusterCentersWithNorm.zipWithIndex).map { case (p, id) =>
        Cluster(id, p.vector)
      }
      spark.createDataFrame(dataRDD).write.parquet(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String): KMeansModel = {
      implicit val formats = DefaultFormats
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)
      val k = (metadata \ "k").extract[Int]
      val centroids = spark.read.parquet(Loader.dataPath(path))
      Loader.checkSchema[Cluster](centroids.schema)
      val localCentroids = centroids.rdd.map(Cluster.apply).collect()
      assert(k == localCentroids.length)
      val distanceMeasure = (metadata \ "distanceMeasure").extract[String]
      val trainingCost = (metadata \ "trainingCost").extract[Double]
      new KMeansModel(localCentroids.sortBy(_.id).map(_.point), distanceMeasure, trainingCost, -1)
    }
  }
}
