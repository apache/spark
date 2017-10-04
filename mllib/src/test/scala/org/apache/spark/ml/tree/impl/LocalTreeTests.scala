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

package org.apache.spark.ml.tree.impl

import org.apache.spark.internal.Logging
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree._
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.rdd.RDD


/** Object providing test-only methods for local decision tree training. */
private[impl] object LocalTreeTests extends Logging {
  /**
   * Returns an array containing a single element; an array of continuous splits for
   * the feature with index featureIndex and the passed-in set of values. Creates one
   * continuous split per value in the array.
   */
  private[impl] def getContinuousSplits(
      values: Array[Int],
      featureIndex: Int): Array[Array[Split]] = {
    val splits = values.sorted.map {
      new ContinuousSplit(featureIndex, _).asInstanceOf[Split]
    }
    Array(splits)
  }
}
