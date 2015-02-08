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

import org.dmg.pmml.Array.Type
import org.dmg.pmml.Cluster
import org.dmg.pmml.ClusteringField
import org.dmg.pmml.ClusteringModel
import org.dmg.pmml.ClusteringModel.ModelClass
import org.dmg.pmml.CompareFunctionType
import org.dmg.pmml.ComparisonMeasure
import org.dmg.pmml.ComparisonMeasure.Kind
import org.dmg.pmml.DataDictionary
import org.dmg.pmml.DataField
import org.dmg.pmml.DataType
import org.dmg.pmml.FieldName
import org.dmg.pmml.FieldUsageType
import org.dmg.pmml.MiningField
import org.dmg.pmml.MiningFunctionType
import org.dmg.pmml.MiningSchema
import org.dmg.pmml.OpType
import org.dmg.pmml.SquaredEuclidean
import org.apache.spark.mllib.clustering.KMeansModel

/**
 * PMML Model Export for KMeansModel class
 */
private[mllib] class KMeansPMMLModelExport(model : KMeansModel) extends PMMLModelExport{

  /**
   * Export the input KMeansModel model to PMML format
   */
  populateKMeansPMML(model)
  
  private def populateKMeansPMML(model : KMeansModel): Unit = {
    
     pmml.getHeader().setDescription("k-means clustering") 
     
     if(model.clusterCenters.length > 0){
       
       val clusterCenter = model.clusterCenters(0)
       
       val fields = new Array[FieldName](clusterCenter.size)
       
       val dataDictionary = new DataDictionary()
       
       val miningSchema = new MiningSchema()
       
       val comparisonMeasure = new ComparisonMeasure()
            .withKind(Kind.DISTANCE)
            .withMeasure(new SquaredEuclidean()
       )
       
       val clusteringModel = new ClusteringModel(miningSchema, comparisonMeasure, 
        MiningFunctionType.CLUSTERING, ModelClass.CENTER_BASED, model.clusterCenters.length)
        .withModelName("k-means")
       
       for ( i <- 0 until clusterCenter.size) {
         fields(i) = FieldName.create("field_" + i)
         dataDictionary
            .withDataFields(new DataField(fields(i), OpType.CONTINUOUS, DataType.DOUBLE))
         miningSchema
            .withMiningFields(new MiningField(fields(i))
            .withUsageType(FieldUsageType.ACTIVE))
         clusteringModel.withClusteringFields(
             new ClusteringField(fields(i)).withCompareFunction(CompareFunctionType.ABS_DIFF)
         )     
       }
       
       dataDictionary.withNumberOfFields((dataDictionary.getDataFields()).size())
       
       for ( i <- 0 until model.clusterCenters.size ) {
         val cluster = new Cluster()
            .withName("cluster_" + i)
            .withArray(new org.dmg.pmml.Array()
            .withType(Type.REAL)
            .withN(clusterCenter.size)
            .withValue(model.clusterCenters(i).toArray.mkString(" ")))
            // we don't have the size of the single cluster but only the centroids (withValue)
            // .withSize(value)
         clusteringModel.withClusters(cluster)
       }
       
       pmml.setDataDictionary(dataDictionary)
       pmml.withModels(clusteringModel)
       
     }
 
  }
  
}
