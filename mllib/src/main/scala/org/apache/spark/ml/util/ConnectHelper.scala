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
import org.apache.spark.ml.feature.{CountVectorizerModel, StringIndexerModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

private[spark] class ConnectHelper(override val uid: String) extends Model[ConnectHelper] {
  def this() = this(Identifiable.randomUID("ConnectHelper"))

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

  override def copy(extra: ParamMap): ConnectHelper = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF()

  override def transformSchema(schema: StructType): StructType = schema

  override def hasParent: Boolean = false
}
