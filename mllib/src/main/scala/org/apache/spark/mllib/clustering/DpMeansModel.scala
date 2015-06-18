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
import scala.collection.mutable

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.pmml.PMMLExportable
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}

/**
 * A clustering model for DP means. Each point belongs to the cluster with the closest center.
 */
class DpMeansModel
    (val clusterCenters: Array[Vector]) extends Saveable with Serializable with PMMLExportable {

  /** A Java-friendly constructor that takes an Iterable of Vectors. */
  def this(centers: java.lang.Iterable[Vector]) = this(centers.asScala.toArray)

  /** Total number of clusters obtained. */
  def k: Int = clusterCenters.length

  /** Returns the cluster index that a given point belongs to. */
  def predict(point: Vector): Int = {
    val centersWithNorm = clusterCentersWithNorm
    DpMeans.assignCluster(centersWithNorm.to[mutable.ArrayBuffer], new VectorWithNorm(point))._1
  }

  /** Maps the points in the given RDD to their closest cluster indices. */
  def predict(points: RDD[Vector]): RDD[Int] = {
    val centersWithNorm = clusterCentersWithNorm
    val bcCentersWithNorm = points.context.broadcast(centersWithNorm)
    points.map(p => DpMeans.assignCluster(bcCentersWithNorm.value.to[mutable.ArrayBuffer],
         new VectorWithNorm(p))._1)
  }

  /**
   * Return the cost (sum of squared distances of points to their nearest center) for this
   * model on the given data.
   */
  def computeCost(data: RDD[Vector]): Double = {
    val centersWithNorm = clusterCentersWithNorm
    val bcCentersWithNorm = data.context.broadcast(centersWithNorm)
    data.map(p => DpMeans.assignCluster(bcCentersWithNorm.value.to[mutable.ArrayBuffer],
        new VectorWithNorm(p))._2).sum()
  }

  private def clusterCentersWithNorm: Iterable[VectorWithNorm] =
    clusterCenters.map(new VectorWithNorm(_))

  override def save(sc: SparkContext, path: String): Unit = {
    DpMeansModel.SaveLoadV1_0.save(sc, this, path)
  }

  override protected def formatVersion: String = "1.0"

}

object DpMeansModel extends Loader[DpMeansModel] {

  private case class Cluster(id: Int, point: Vector)

  private object Cluster {
    def apply(r: Row): Cluster = {
      Cluster(r.getInt(0), r.getAs[Vector](1))
    }
  }

  private object SaveLoadV1_0 {

    val thisFormatVersion = "1.0"

    val thisClassName = "org.apache.spark.mllib.clustering.DpMeansModel"

    def save(sc: SparkContext, model: DpMeansModel, path: String): Unit = {

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      // Create JSON metadata.
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~ ("k" -> model.k)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      // Create Parquet data.
      val dataRDD = sc.parallelize(model.clusterCenters.zipWithIndex).map { case (point, id) =>
        Cluster(id, point)
      }
      dataRDD.toDF().write.parquet(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String): DpMeansModel = {
      implicit val formats = DefaultFormats
      val dataPath = Loader.dataPath(path)
      val sqlContext = new SQLContext(sc)
      val dataFrame = sqlContext.parquetFile(dataPath)

      // Check schema explicitly since erasure makes it hard to use match-case for checking.
      Loader.checkSchema[Cluster](dataFrame.schema)

      val localCentroids = dataFrame.map(Cluster.apply).collect()
      return new DpMeansModel(localCentroids.sortBy(_.id).map(_.point))
    }
  }

    override def load(sc: SparkContext, path: String) : DpMeansModel = {
      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      implicit val formats = DefaultFormats
      val k = (metadata \ "k").extract[Int]
      (className, formatVersion) match {
        case(SaveLoadV1_0.thisClassName, "1.0") => {
          val model = SaveLoadV1_0.load(sc, path)
          require(model.k == k,
            s"DpMeansModel requires $k clusters " +
              s"got ${model.k} clusters instead")
          model
        }
        case _ => throw new Exception(
          s"DpMeansModel.load did not recognize model with (className, format version):" +
            s"($className, $formatVersion).  Supported:\n" +
            s"  ($SaveLoadV1_0.thisClassName, $SaveLoadV1_0.thisFormatVersion)")
        }
      }

  }

