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

import org.dmg.pmml.RegressionNormalizationMethodType

import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.regression.LassoModel
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.RidgeRegressionModel

private[mllib] object PMMLModelExportFactory {
  
  /**
   * Factory object to help creating the necessary PMMLModelExport implementation 
   * taking as input the machine learning model (for example KMeansModel).
   */
  def createPMMLModelExport(model: Any): PMMLModelExport = {
    model match {
      case kmeans: KMeansModel =>
        new KMeansPMMLModelExport(kmeans)
      case linear: LinearRegressionModel =>
        new GeneralizedLinearPMMLModelExport(linear, "linear regression")
      case ridge: RidgeRegressionModel =>
        new GeneralizedLinearPMMLModelExport(ridge, "ridge regression")
      case lasso: LassoModel =>
        new GeneralizedLinearPMMLModelExport(lasso, "lasso regression")
      case svm: SVMModel =>
        new BinaryClassificationPMMLModelExport(
          svm, "linear SVM", RegressionNormalizationMethodType.NONE, 
          svm.getThreshold.getOrElse(0.0))
      case logistic: LogisticRegressionModel =>
        if (logistic.numClasses == 2) {
          new BinaryClassificationPMMLModelExport(
            logistic, "logistic regression", RegressionNormalizationMethodType.LOGIT,
            logistic.getThreshold.getOrElse(0.5))
        } else {
          throw new IllegalArgumentException(
            "PMML Export not supported for Multinomial Logistic Regression")
        }
      case _ =>
        throw new IllegalArgumentException(
          "PMML Export not supported for model: " + model.getClass.getName)
    }
  }
  
}
