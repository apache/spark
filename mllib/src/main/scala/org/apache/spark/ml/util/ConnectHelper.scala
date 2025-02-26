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
package org.apache.spark.ml.util

import org.apache.spark.ml.Model
import org.apache.spark.ml.clustering.PowerIterationClustering
import org.apache.spark.ml.feature._
import org.apache.spark.ml.fpm.PrefixSpan
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.stat._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

private[spark] class ConnectHelper(override val uid: String) extends Model[ConnectHelper] {
  def this() = this(Identifiable.randomUID("ConnectHelper"))

  def handleOverwrite(path: String, shouldOverwrite: Boolean): Boolean = {
    val spark = SparkSession.builder().getOrCreate()
    new FileSystemOverwrite().handleOverwrite(path, shouldOverwrite, spark)
    true
  }

  def stringIndexerModelFromLabels(
      uid: String, labels: Array[String]): StringIndexerModel = {
    new StringIndexerModel(uid, labels)
  }

  def stringIndexerModelFromLabelsArray(
      uid: String, labelsArray: Array[Array[String]]): StringIndexerModel = {
    new StringIndexerModel(uid, labelsArray)
  }

  def countVectorizerModelFromVocabulary(
      uid: String, vocabulary: Array[String]): CountVectorizerModel = {
    new CountVectorizerModel(uid, vocabulary)
  }

  def stopWordsRemoverLoadDefaultStopWords(language: String): Array[String] = {
    StopWordsRemover.loadDefaultStopWords(language)
  }

  def stopWordsRemoverGetDefaultOrUS: String = {
    StopWordsRemover.getDefaultOrUS.toString
  }

  def chiSquareTest(
      dataset: DataFrame,
      featuresCol: String,
      labelCol: String,
      flatten: Boolean): DataFrame = {
    ChiSquareTest.test(dataset, featuresCol, labelCol, flatten)
  }

  def correlation(
      dataset: DataFrame,
      column: String,
      method: String): DataFrame = {
    Correlation.corr(dataset, column, method)
  }

  def kolmogorovSmirnovTest(
      dataset: DataFrame,
      sampleCol: String,
      distName: String,
      params: Array[Double]): DataFrame = {
    KolmogorovSmirnovTest.test(dataset, sampleCol, distName, params.toIndexedSeq: _*)
  }

  def powerIterationClusteringAssignClusters(
      dataset: DataFrame,
      k: Int,
      maxIter: Int,
      initMode: String,
      srcCol: String,
      dstCol: String,
      weightCol: String): DataFrame = {
    val pic = new PowerIterationClustering()
      .setK(k)
      .setMaxIter(maxIter)
      .setInitMode(initMode)
      .setSrcCol(srcCol)
      .setDstCol(dstCol)
    if (weightCol.nonEmpty) {
      pic.setWeightCol(weightCol)
    }
    pic.assignClusters(dataset)
  }

  def prefixSpanFindFrequentSequentialPatterns(
      dataset: DataFrame,
      minSupport: Double,
      maxPatternLength: Int,
      maxLocalProjDBSize: Long,
      sequenceCol: String): DataFrame = {
    val prefixSpan = new PrefixSpan()
      .setMinSupport(minSupport)
      .setMaxPatternLength(maxPatternLength)
      .setMaxLocalProjDBSize(maxLocalProjDBSize)
      .setSequenceCol(sequenceCol)
    prefixSpan.findFrequentSequentialPatterns(dataset)
  }


  override def copy(extra: ParamMap): ConnectHelper = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF()

  override def transformSchema(schema: StructType): StructType = schema

  override def hasParent: Boolean = false
}
