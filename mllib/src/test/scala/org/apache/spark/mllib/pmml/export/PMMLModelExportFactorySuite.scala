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

import org.scalatest.FunSuite
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LassoModel
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.RidgeRegressionModel
import org.apache.spark.mllib.util.LinearDataGenerator

class PMMLModelExportFactorySuite extends FunSuite{

   test("PMMLModelExportFactory create KMeansPMMLModelExport when passing a KMeansModel") {
    
    //arrange
    val clusterCenters = Array(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0)
    )
    val kmeansModel = new KMeansModel(clusterCenters);
    
    //act
    val modelExport = PMMLModelExportFactory.createPMMLModelExport(kmeansModel)
         
    //assert
    assert(modelExport.isInstanceOf[KMeansPMMLModelExport])
   
   }
   
   test("PMMLModelExportFactory create GeneralizedLinearPMMLModelExport when passing a "
       +"LinearRegressionModel, RidgeRegressionModel, LassoModel or SVMModel") {
    
    //arrange
    val linearInput = LinearDataGenerator.generateLinearInput(
      3.0, Array(10.0, 10.0), 1, 17)
    val linearRegressionModel = new LinearRegressionModel(linearInput(0).features, linearInput(0).label)
    val ridgeRegressionModel = new RidgeRegressionModel(linearInput(0).features, linearInput(0).label)
    val lassoModel = new LassoModel(linearInput(0).features, linearInput(0).label)
    val svmModel = new SVMModel(linearInput(0).features, linearInput(0).label)
    
    //act
    val linearModelExport = PMMLModelExportFactory.createPMMLModelExport(linearRegressionModel)         
    //assert
    assert(linearModelExport.isInstanceOf[GeneralizedLinearPMMLModelExport])

    //act
    val ridgeModelExport = PMMLModelExportFactory.createPMMLModelExport(ridgeRegressionModel)         
    //assert
    assert(ridgeModelExport.isInstanceOf[GeneralizedLinearPMMLModelExport])
    
    //act
    val lassoModelExport = PMMLModelExportFactory.createPMMLModelExport(lassoModel)         
    //assert
    assert(lassoModelExport.isInstanceOf[GeneralizedLinearPMMLModelExport])
    
    //act
    val svmModelExport = PMMLModelExportFactory.createPMMLModelExport(svmModel)         
    //assert
    assert(svmModelExport.isInstanceOf[GeneralizedLinearPMMLModelExport])
    
   }
   
   test("PMMLModelExportFactory create LogisticRegressionPMMLModelExport when passing a LogisticRegressionModel") {
    
    //arrange
    val linearInput = LinearDataGenerator.generateLinearInput(
      3.0, Array(10.0, 10.0), 1, 17)
    val logisticRegressionModel = new LogisticRegressionModel(linearInput(0).features, linearInput(0).label);

    //act
    val logisticRegressionModelExport = PMMLModelExportFactory.createPMMLModelExport(logisticRegressionModel)         
    //assert
    assert(logisticRegressionModelExport.isInstanceOf[LogisticRegressionPMMLModelExport])
   
   }
   
   test("PMMLModelExportFactory throw IllegalArgumentException when passing an unsupported model") {
    
    //arrange
    val invalidModel = new Object;
    
    //assert
    intercept[IllegalArgumentException] {
        //act
    	PMMLModelExportFactory.createPMMLModelExport(invalidModel)
    }
   
   }
  
}
