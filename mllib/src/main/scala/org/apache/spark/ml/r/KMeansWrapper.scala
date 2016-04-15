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

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class KMeansWrapper private (
    pipeline: PipelineModel) {

  private val kMeansModel: KMeansModel = pipeline.stages(1).asInstanceOf[KMeansModel]

  lazy val coefficients: Array[Double] = kMeansModel.clusterCenters.flatMap(_.toArray)

  private lazy val attrs = AttributeGroup.fromStructField(
    kMeansModel.summary.predictions.schema(kMeansModel.getFeaturesCol))

  lazy val features: Array[String] = attrs.attributes.get.map(_.name.get)

  lazy val k: Int = kMeansModel.getK

  lazy val size: Array[Long] = kMeansModel.summary.clusterSizes

  lazy val cluster: DataFrame = kMeansModel.summary.cluster

  def fitted(method: String): DataFrame = {
    if (method == "centers") {
      kMeansModel.summary.predictions.drop(kMeansModel.getFeaturesCol)
    } else if (method == "classes") {
      kMeansModel.summary.cluster
    } else {
      throw new UnsupportedOperationException(
        s"Method (centers or classes) required but $method found.")
    }
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset).drop(kMeansModel.getFeaturesCol)
  }

}

private[r] object KMeansWrapper {

  def fit(
      data: DataFrame,
      k: Double,
      maxIter: Double,
      initMode: String,
      columns: Array[String]): KMeansWrapper = {

    val assembler = new VectorAssembler()
      .setInputCols(columns)
      .setOutputCol("features")

    val kMeans = new KMeans()
      .setK(k.toInt)
      .setMaxIter(maxIter.toInt)
      .setInitMode(initMode)

    val pipeline = new Pipeline()
      .setStages(Array(assembler, kMeans))
      .fit(data)

    new KMeansWrapper(pipeline)
  }
}
