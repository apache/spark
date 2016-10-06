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

package org.apache.spark.ml.r

import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.ml.util.MLReader

/**
 * This is the Scala stub of SparkR read.ml. It will dispatch the call to corresponding
 * model wrapper loading function according the class name extracted from rMetadata of the path.
 */
private[r] object RWrappers extends MLReader[Object] {

  override def load(path: String): Object = {
    implicit val format = DefaultFormats
    val rMetadataPath = new Path(path, "rMetadata").toString
    val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
    val rMetadata = parse(rMetadataStr)
    val className = (rMetadata \ "class").extract[String]
    className match {
      case "org.apache.spark.ml.r.NaiveBayesWrapper" => NaiveBayesWrapper.load(path)
      case "org.apache.spark.ml.r.AFTSurvivalRegressionWrapper" =>
        AFTSurvivalRegressionWrapper.load(path)
      case "org.apache.spark.ml.r.GeneralizedLinearRegressionWrapper" =>
        GeneralizedLinearRegressionWrapper.load(path)
      case "org.apache.spark.ml.r.KMeansWrapper" =>
        KMeansWrapper.load(path)
      case "org.apache.spark.ml.r.MultilayerPerceptronClassifierWrapper" =>
        MultilayerPerceptronClassifierWrapper.load(path)
      case "org.apache.spark.ml.r.LDAWrapper" =>
        LDAWrapper.load(path)
      case "org.apache.spark.ml.r.IsotonicRegressionWrapper" =>
        IsotonicRegressionWrapper.load(path)
      case "org.apache.spark.ml.r.GaussianMixtureWrapper" =>
        GaussianMixtureWrapper.load(path)
      case "org.apache.spark.ml.r.ALSWrapper" =>
        ALSWrapper.load(path)
      case "org.apache.spark.ml.r.DecisionTreeRegressorWrapper" =>
        DecisionTreeRegressorWrapper.load(path)
      case "org.apache.spark.ml.r.DecisionTreeClassifierWrapper" =>
        DecisionTreeClassifierWrapper.load(path)
      case _ =>
        throw new SparkException(s"SparkR read.ml does not support load $className")
    }
  }
}
