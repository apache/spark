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

package org.apache.spark.ml.evaluation

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.{DenseVector, Vector, VectorElementWiseSum}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count}

private[evaluation] object CosineSilhouette {

  case class ClusterStats(omega: Vector, count: Long)

  private[this] var kryoRegistrationPerformed: Boolean = false

  /**
    * This method registers the class
    * [[org.apache.spark.ml.evaluation.CosineSilhouette.ClusterStats]]
    * for kryo serialization.
    *
    * @param sc `SparkContext` to be used
    */
  def registerKryoClasses(sc: SparkContext): Unit = {
    if (! kryoRegistrationPerformed) {
      sc.getConf.registerKryoClasses(
        Array(
          classOf[CosineSilhouette.ClusterStats]
        )
      )
      kryoRegistrationPerformed = true
    }
  }

  def computeOmegaAndCount(
      df: DataFrame,
      predictionCol: String,
      featuresCol: String): DataFrame = {
    val omegaUdaf = new VectorElementWiseSum()
    df.groupBy(predictionCol)
      .agg(
        count("*").alias("count"),
        omegaUdaf(col("csi")).alias("omega")
      )
  }

  def computeCsi(vector: Vector): Vector = {
    var sum: Double = 0.0
    vector.foreachActive( (_, i) => {
      sum += i * i
    })
    val norm = math.sqrt(sum)
    new DenseVector(vector.toArray.map( _ / norm ))
  }

  def computeCosineSilhouetteCoefficient(
      broadcastedClustersMap: Broadcast[Map[Int, ClusterStats]],
      vector: Vector,
      clusterId: Int,
      csi: Vector): Double = {

    def compute(point: Vector, csi: Vector, clusterStats: ClusterStats): Double = {
      var omegaMultiplyCsiSum: Double = 0.0
      csi.foreachActive( (i, iCsi) => {
        omegaMultiplyCsiSum += clusterStats.omega(i) * iCsi
      })

      1 - omegaMultiplyCsiSum / clusterStats.count
    }

    var minOther = Double.MaxValue
    for(c <- broadcastedClustersMap.value.keySet) {
      if (c != clusterId) {
        val sil = compute(vector, csi, broadcastedClustersMap.value(c))
        if(sil < minOther) {
          minOther = sil
        }
      }
    }
    val clusterCurrentPoint = broadcastedClustersMap.value(clusterId)
    // adjustment for excluding the node itself from
    // the computation of the average dissimilarity
    val clusterSil = if (clusterCurrentPoint.count == 1) {
      0
    } else {
      compute(vector, csi, clusterCurrentPoint) * clusterCurrentPoint.count /
        (clusterCurrentPoint.count - 1)
    }


    var silhouetteCoeff = 0.0
    if (clusterSil < minOther) {
      silhouetteCoeff = 1 - (clusterSil / minOther)
    } else {
      if (clusterSil > minOther) {
        silhouetteCoeff = (minOther / clusterSil) - 1
      }
    }

    silhouetteCoeff

  }

}
