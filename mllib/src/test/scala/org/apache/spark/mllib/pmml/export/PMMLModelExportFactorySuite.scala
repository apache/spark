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

class PMMLModelExportFactorySuite extends FunSuite {

  test("PMMLModelExportFactory create KMeansPMMLModelExport when passing a KMeansModel") {
    val clusterCenters = Array(
      Vectors.dense(1.0, 2.0, 6.0),
      Vectors.dense(1.0, 3.0, 0.0),
      Vectors.dense(1.0, 4.0, 6.0))
    val kmeansModel = new KMeansModel(clusterCenters)

    val modelExport = PMMLModelExportFactory.createPMMLModelExport(kmeansModel)
         
    assert(modelExport.isInstanceOf[KMeansPMMLModelExport])
   }
   
   test("PMMLModelExportFactory create GeneralizedLinearPMMLModelExport when passing a "
       + "LinearRegressionModel, RidgeRegressionModel, LassoModel or SVMModel") {
     val linearInput = LinearDataGenerator.generateLinearInput(3.0, Array(10.0, 10.0), 1, 17)

     val linearRegressionModel =
       new LinearRegressionModel(linearInput(0).features, linearInput(0).label)
     val linearModelExport = PMMLModelExportFactory.createPMMLModelExport(linearRegressionModel)
     assert(linearModelExport.isInstanceOf[GeneralizedLinearPMMLModelExport])

     val ridgeRegressionModel =
       new RidgeRegressionModel(linearInput(0).features, linearInput(0).label)
     val ridgeModelExport = PMMLModelExportFactory.createPMMLModelExport(ridgeRegressionModel)
     assert(ridgeModelExport.isInstanceOf[GeneralizedLinearPMMLModelExport])


     val lassoModel = new LassoModel(linearInput(0).features, linearInput(0).label)
     val lassoModelExport = PMMLModelExportFactory.createPMMLModelExport(lassoModel)
     assert(lassoModelExport.isInstanceOf[GeneralizedLinearPMMLModelExport])

     val svmModel = new SVMModel(linearInput(0).features, linearInput(0).label)
     val svmModelExport = PMMLModelExportFactory.createPMMLModelExport(svmModel)
     assert(svmModelExport.isInstanceOf[GeneralizedLinearPMMLModelExport])
   }

  test("PMMLModelExportFactory create LogisticRegressionPMMLModelExport "
    + "when passing a LogisticRegressionModel") {
    val linearInput = LinearDataGenerator.generateLinearInput(3.0, Array(10.0, 10.0), 1, 17)
    val logisticRegressionModel =
      new LogisticRegressionModel(linearInput(0).features, linearInput(0).label)

    val logisticRegressionModelExport =
      PMMLModelExportFactory.createPMMLModelExport(logisticRegressionModel)

    assert(logisticRegressionModelExport.isInstanceOf[LogisticRegressionPMMLModelExport])
   }
   
   test("PMMLModelExportFactory throw IllegalArgumentException "
       + "when passing an unsupported model") {
    val invalidModel = new Object
    
    intercept[IllegalArgumentException] {
      PMMLModelExportFactory.createPMMLModelExport(invalidModel)
    }
   }
}
