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

package org.apache.spark.sql.connect.ml

import org.apache.spark.connect.proto
import org.apache.spark.ml
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.classification.TrainingSummary
import org.apache.spark.sql.DataFrame


object AlgorithmRegistry {

  def get(name: String): Algorithm = {
    name match {
      case "LogisticRegression" => new LogisticRegressionAlgorithm
      case _ =>
        throw new IllegalArgumentException()
    }
  }

}


abstract class Algorithm {

  def initiateEstimator(uid: String): Estimator[_]

  def getModelAttr(model: Model[_], name: String): Either[proto.MlCommandResponse, DataFrame]

  def getModelSummaryAttr(
    model: Model[_],
    name: String,
    datasetOpt: Option[DataFrame]
  ): Either[proto.MlCommandResponse, DataFrame]
}

class LogisticRegressionAlgorithm extends Algorithm {

  def initiateEstimator(uid: String): Estimator[_] = {
    new ml.classification.LogisticRegression(uid)
  }

  def getModelAttr(model: Model[_], name: String): Either[proto.MlCommandResponse, DataFrame] = {
    val lorModel = model.asInstanceOf[ml.classification.LogisticRegressionModel]
    // TODO: hasSummary
    name match {
      case "numClasses" => Left(Serializer.serialize(lorModel.numClasses))
      case "numFeatures" => Left(Serializer.serialize(lorModel.numFeatures))
      case "intercept" => Left(Serializer.serialize(lorModel.intercept))
      case "interceptVector" => Left(Serializer.serialize(lorModel.interceptVector))
      case "coefficients" => Left(Serializer.serialize(lorModel.coefficients))
      case "coefficientMatrix" => Left(Serializer.serialize(lorModel.coefficientMatrix))
      case _ =>
        throw new IllegalArgumentException()
    }
  }

  def getModelSummaryAttr(
                           model: Model[_],
                           name: String,
                           datasetOpt: Option[DataFrame]
                         ): Either[proto.MlCommandResponse, DataFrame] = {
    val lorModel = model.asInstanceOf[ml.classification.LogisticRegressionModel]
    val summary = if (datasetOpt.isDefined) {
      lorModel.evaluate(datasetOpt.get)
    } else {
      lorModel.summary
    }
    val attrValueOpt = if (lorModel.numClasses <= 2) {
      SummaryUtils.getBinaryClassificationSummaryAttr(summary.asBinary, name)
    } else {
      SummaryUtils.getClassificationSummaryAttr(summary, name)
    }
    attrValueOpt.orElse(
      if (datasetOpt.isEmpty) {
        SummaryUtils.getTrainingSummaryAttr(summary.asInstanceOf[TrainingSummary], name)
      } else None
    ).orElse {
      val lorSummary = summary
      name match {
        case "probabilityCol" => Some(Left(Serializer.serialize(lorSummary.probabilityCol)))
        case "featuresCol" => Some(Left(Serializer.serialize(lorSummary.featuresCol)))
        case _ =>
          throw new IllegalArgumentException()
      }
    }.get
  }
}
