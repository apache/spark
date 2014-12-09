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

import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.export.ModelExportType.ModelExportType
import org.apache.spark.mllib.export.ModelExportType.PMML
import org.apache.spark.mllib.export.pmml.GeneralizedLinearPMMLModelExport
import org.apache.spark.mllib.export.pmml.KMeansPMMLModelExport
import org.apache.spark.mllib.export.pmml.LogisticRegressionPMMLModelExport
import org.apache.spark.mllib.regression.LassoModel
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.RidgeRegressionModel

private[mllib] object ModelExportFactory {
  
  /**
   * Factory object to help creating the necessary ModelExport implementation 
   * taking as input the ModelExportType (for example PMML) 
   * and the machine learning model (for example KMeansModel).
   */
  def createModelExport(model: Any, exportType: ModelExportType): ModelExport = {
    return exportType match{
      case PMML => model match{
        case kmeans: KMeansModel => 
          new KMeansPMMLModelExport(kmeans)
        case linearRegression: LinearRegressionModel => 
          new GeneralizedLinearPMMLModelExport(linearRegression, "linear regression")
        case ridgeRegression: RidgeRegressionModel => 
          new GeneralizedLinearPMMLModelExport(ridgeRegression, "ridge regression")
        case lassoRegression: LassoModel => 
          new GeneralizedLinearPMMLModelExport(lassoRegression, "lasso regression")
        case svm: SVMModel => 
          new GeneralizedLinearPMMLModelExport(
              svm, 
              "linear SVM: if predicted value > 0, the outcome is positive, or negative otherwise")
        case logisticRegression: LogisticRegressionModel => 
          new LogisticRegressionPMMLModelExport(
              logisticRegression, 
              "logistic regression: if predicted value > 0.5, "
              + "the outcome is positive, or negative otherwise")
        case _ => 
          throw new IllegalArgumentException("Export not supported for model: " + model.getClass)
      }
      case _ => throw new IllegalArgumentException("Export type not supported:" + exportType)
      }
  }
  
}
