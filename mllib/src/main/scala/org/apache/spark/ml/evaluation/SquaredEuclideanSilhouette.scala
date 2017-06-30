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
import org.apache.spark.ml.linalg.{Vector, VectorElementWiseSum}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, sum}

private[evaluation] object SquaredEuclideanSilhouette {

  private[this] var kryoRegistrationPerformed: Boolean = false

  /**
    * This method registers the class
    * [[org.apache.spark.ml.evaluation.SquaredEuclideanSilhouette.ClusterStats]]
    * for kryo serialization.
 *
    * @param sc `SparkContext` to be used
    */
  def registerKryoClasses(sc: SparkContext): Unit = {
    if (! kryoRegistrationPerformed) {
      sc.getConf.registerKryoClasses(
        Array(
          classOf[SquaredEuclideanSilhouette.ClusterStats]
        )
      )
      kryoRegistrationPerformed = true
    }
  }

  case class ClusterStats(Y: Vector, psi: Double, count: Long)
  
  def computeCsi(vector: Vector): Double = {
    var sumOfSquares = 0.0
    vector.foreachActive((_, v) => {
      sumOfSquares += v * v
    })
    sumOfSquares
  }
  
  def computeYVectorPsiAndCount(
      df: DataFrame,
      predictionCol: String,
      featuresCol: String): DataFrame = {
    val Yudaf = new VectorElementWiseSum()
    df.groupBy(predictionCol)
      .agg(
        count("*").alias("count"),
        sum("csi").alias("psi"),
        Yudaf(col(featuresCol)).alias("y")
      )
  }

  def computeSquaredSilhouetteCoefficient(
      broadcastedClustersMap: Broadcast[Map[Int, ClusterStats]],
      vector: Vector,
      clusterId: Int,
      csi: Double): Double = {

    def compute(csi: Double, point: Vector, clusterStats: ClusterStats): Double = {
      var YmultiplyPoint = 0.0
      point.foreachActive((idx, v) => {
        YmultiplyPoint += clusterStats.Y(idx) * v
      })

      csi + clusterStats.psi / clusterStats.count - 2 * YmultiplyPoint / clusterStats.count
    }

    var minOther = Double.MaxValue
    for(c <- broadcastedClustersMap.value.keySet) {
      if (c != clusterId) {
        val sil = compute(csi, vector, broadcastedClustersMap.value(c))
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
      compute(csi, vector, clusterCurrentPoint) * clusterCurrentPoint.count /
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