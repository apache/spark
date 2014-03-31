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

package org.apache.spark.mllib

import org.apache.spark.SparkContext

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * Provides methods related to machine learning on top of [[org.apache.spark.SparkContext]].
 *
 * @param sparkContext a [[org.apache.spark.SparkContext]] instance
 */
class MLContext(val sparkContext: SparkContext) {
  /**
   * Reads labeled data in the LIBSVM format into an RDD[LabeledPoint].
   * The LIBSVM format is a text-based format used by LIBSVM and LIBLINEAR.
   * Each line represents a labeled sparse feature vector using the following format:
   * {{{label index1:value1 index2:value2 ...}}}
   * where the indices are one-based and in ascending order.
   * This method parses each line into a [[org.apache.spark.mllib.regression.LabeledPoint]],
   * where the feature indices are converted to zero-based.
   *
   * @param path file or directory path in any Hadoop-supported file system URI
   * @param numFeatures number of features, which will be determined from the input data if a
   *                    non-positive value is given. The default value is 0.
   * @param labelParser parser for labels, default: _.toDouble
   * @return labeled data stored as an RDD[LabeledPoint]
   */
  def libSVMFile(
      path: String,
      numFeatures: Int = 0,
      labelParser: String => Double = _.toDouble): RDD[LabeledPoint] = {
    val parsed = sparkContext.textFile(path).map(_.trim).filter(!_.isEmpty).map(_.split(' '))
    // Determine number of features.
    val d = if (numFeatures > 0) {
      numFeatures
    } else {
      parsed.map { items =>
        if (items.length > 1) {
          items.last.split(':')(0).toInt
        } else {
          0
        }
      }.reduce(math.max)
    }
    parsed.map { items =>
      val label = labelParser(items.head)
      val (indices, values) = items.tail.map { item =>
        val indexAndValue = item.split(':')
        val index = indexAndValue(0).toInt - 1
        val value = indexAndValue(1).toDouble
        (index, value)
      }.unzip
      LabeledPoint(label, Vectors.sparse(d, indices.toArray, values.toArray))
    }
  }
}

object MLContext {
  /**
   * Creates an [[org.apache.spark.mllib.MLContext]] instance from
   * an [[org.apache.spark.SparkContext]] instance.
   */
  def apply(sc: SparkContext): MLContext = new MLContext(sc)
}
