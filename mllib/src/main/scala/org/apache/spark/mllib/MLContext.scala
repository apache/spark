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

class MLContext(self: SparkContext) {
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
   * @param numFeatures number of features
   * @param labelParser parser for labels, default: _.toDouble
   * @return labeled data stored as an RDD[LabeledPoint]
   */
  def libSVMFile(
      path: String,
      numFeatures: Int,
      labelParser: String => Double = _.toDouble): RDD[LabeledPoint] = {
    self.textFile(path).map(_.trim).filter(!_.isEmpty).map { line =>
      val items = line.split(' ')
      val label = labelParser(items.head)
      val features = Vectors.sparse(numFeatures, items.tail.map { item =>
        val indexAndValue = item.split(':')
        val index = indexAndValue(0).toInt - 1
        val value = indexAndValue(1).toDouble
        (index, value)
      })
      LabeledPoint(label, features)
    }
  }
}

object MLContext {
  implicit def sparkContextToMLContext(sc: SparkContext): MLContext = new MLContext(sc)
}
