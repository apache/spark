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

package org.apache.spark.mllib.pmml.export

import scala.{Array => SArray}

import org.dmg.pmml.{Array, CompareFunction, ComparisonMeasure, DataDictionary, DataField, DataType,
  FieldName, MiningField, MiningFunction, MiningSchema, OpType, SquaredEuclidean}
import org.dmg.pmml.clustering.{Cluster, ClusteringField, ClusteringModel}

import org.apache.spark.mllib.clustering.KMeansModel

/**
 * PMML Model Export for KMeansModel class
 */
private[mllib] class KMeansPMMLModelExport(model: KMeansModel) extends PMMLModelExport {

  populateKMeansPMML(model)

  /**
   * Export the input KMeansModel model to PMML format.
   */
  private def populateKMeansPMML(model: KMeansModel): Unit = {
    pmml.getHeader.setDescription("k-means clustering")

    if (model.clusterCenters.length > 0) {
      val clusterCenter = model.clusterCenters(0)
      val fields = new SArray[FieldName](clusterCenter.size)
      val dataDictionary = new DataDictionary
      val miningSchema = new MiningSchema
      val comparisonMeasure = new ComparisonMeasure()
        .setKind(ComparisonMeasure.Kind.DISTANCE)
        .setMeasure(new SquaredEuclidean())
      val clusteringModel = new ClusteringModel()
        .setModelName("k-means")
        .setMiningSchema(miningSchema)
        .setComparisonMeasure(comparisonMeasure)
        .setMiningFunction(MiningFunction.CLUSTERING)
        .setModelClass(ClusteringModel.ModelClass.CENTER_BASED)
        .setNumberOfClusters(model.clusterCenters.length)

      for (i <- 0 until clusterCenter.size) {
        fields(i) = FieldName.create("field_" + i)
        dataDictionary.addDataFields(new DataField(fields(i), OpType.CONTINUOUS, DataType.DOUBLE))
        miningSchema
          .addMiningFields(new MiningField(fields(i))
          .setUsageType(MiningField.UsageType.ACTIVE))
        clusteringModel.addClusteringFields(
          new ClusteringField(fields(i)).setCompareFunction(CompareFunction.ABS_DIFF))
      }

      dataDictionary.setNumberOfFields(dataDictionary.getDataFields.size)

      for (i <- model.clusterCenters.indices) {
        val cluster = new Cluster()
          .setName("cluster_" + i)
          .setArray(new org.dmg.pmml.Array()
          .setType(Array.Type.REAL)
          .setN(clusterCenter.size)
          .setValue(model.clusterCenters(i).toArray.mkString(" ")))
        // we don't have the size of the single cluster but only the centroids (withValue)
        // .withSize(value)
        clusteringModel.addClusters(cluster)
      }

      pmml.setDataDictionary(dataDictionary)
      pmml.addModels(clusteringModel)
    }
  }
}
