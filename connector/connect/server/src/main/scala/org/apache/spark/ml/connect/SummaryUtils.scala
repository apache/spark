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

package org.apache.spark.ml.connect

import org.apache.spark.connect.proto
import org.apache.spark.ml.classification.{BinaryClassificationSummary, ClassificationSummary, TrainingSummary}
import org.apache.spark.sql.DataFrame

object SummaryUtils {

  def getClassificationSummaryAttr(
      summary: ClassificationSummary,
      name: String): Option[Either[proto.MlCommandResponse, DataFrame]] = {
    name match {
      case "predictions" => Some(Right(summary.predictions))
      case "predictionCol" => Some(Left(Serializer.serializeResponseValue(summary.predictionCol)))
      case "labelCol" => Some(Left(Serializer.serializeResponseValue(summary.labelCol)))
      case "weightCol" => Some(Left(Serializer.serializeResponseValue(summary.weightCol)))
      case "labels" => Some(Left(Serializer.serializeResponseValue(summary.labels)))
      case "truePositiveRateByLabel" =>
        Some(Left(Serializer.serializeResponseValue(summary.truePositiveRateByLabel)))
      case "falsePositiveRateByLabel" =>
        Some(Left(Serializer.serializeResponseValue(summary.falsePositiveRateByLabel)))
      case "precisionByLabel" =>
        Some(Left(Serializer.serializeResponseValue(summary.precisionByLabel)))
      case "recallByLabel" =>
        Some(Left(Serializer.serializeResponseValue(summary.recallByLabel)))
      // TODO: Support beta params.
      case "fMeasureByLabel" =>
        Some(Left(Serializer.serializeResponseValue(summary.fMeasureByLabel)))
      case "accuracy" => Some(Left(Serializer.serializeResponseValue(summary.accuracy)))
      case "weightedTruePositiveRate" =>
        Some(Left(Serializer.serializeResponseValue(summary.weightedTruePositiveRate)))
      case "weightedFalsePositiveRate" =>
        Some(Left(Serializer.serializeResponseValue(summary.weightedFalsePositiveRate)))
      case "weightedRecall" => Some(Left(Serializer.serializeResponseValue(summary.weightedRecall)))
      case "weightedPrecision" =>
        Some(Left(Serializer.serializeResponseValue(summary.weightedPrecision)))
      // TODO: Support beta params.
      case "weightedFMeasure" =>
        Some(Left(Serializer.serializeResponseValue(summary.weightedFMeasure)))
      case _ => None
    }
  }

  def getBinaryClassificationSummaryAttr(
      summary: BinaryClassificationSummary,
      name: String): Option[Either[proto.MlCommandResponse, DataFrame]] = {
    getClassificationSummaryAttr(summary, name).orElse(name match {
      case "scoreCol" => Some(Left(Serializer.serializeResponseValue(summary.scoreCol)))
      case "roc" => Some(Right(summary.roc))
      case "areaUnderROC" => Some(Left(Serializer.serializeResponseValue(summary.areaUnderROC)))
      case "pr" => Some(Right(summary.pr))
      case "fMeasureByThreshold" => Some(Right(summary.fMeasureByThreshold))
      case "precisionByThreshold" => Some(Right(summary.precisionByThreshold))
      case "recallByThreshold" => Some(Right(summary.recallByThreshold))
      case _ => None
    })
  }

  def getTrainingSummaryAttr(
      summary: TrainingSummary,
      name: String): Option[Either[proto.MlCommandResponse, DataFrame]] = {
    name match {
      case "objectiveHistory" =>
        Some(Left(Serializer.serializeResponseValue(summary.objectiveHistory)))
      case "totalIterations" =>
        Some(Left(Serializer.serializeResponseValue(summary.totalIterations)))
      case _ => None
    }
  }

}
