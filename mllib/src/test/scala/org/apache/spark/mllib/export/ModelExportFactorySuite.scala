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

package org.apache.spark.mllib.export

import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest.FunSuite
import org.apache.spark.mllib.export.pmml.KMeansPMMLModelExport

class ModelExportFactorySuite extends FunSuite{

   test("ModelExportFactory create KMeansPMMLModelExport when passing a KMeansModel") {
    
    val clusterCenters = Array(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0)
    )
    
    val kmeansModel = new KMeansModel(clusterCenters);
    
    val modelExport = ModelExportFactory.createModelExport(kmeansModel, ModelExportType.PMML)
         
    assert(modelExport.isInstanceOf[KMeansPMMLModelExport])
   
   }
   
   test("ModelExportFactory throws IllegalArgumentException when passing an unsupported model") {
    
    val invalidModel = new Object;
    
    intercept[IllegalArgumentException] {
    	ModelExportFactory.createModelExport(invalidModel, ModelExportType.PMML)
    }
   
   }
  
}
