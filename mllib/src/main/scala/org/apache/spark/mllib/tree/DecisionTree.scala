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

package org.apache.spark.mllib.tree

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.{Split, Bin, DecisionTreeModel}


class DecisionTree(val strategy : Strategy) {

  def train(input : RDD[LabeledPoint]) : DecisionTreeModel = {

    //Cache input RDD for speedup during multiple passes
    input.cache()

    //TODO: Find all splits and bins using quantiles including support for categorical features, single-pass
    val (splits, bins) = DecisionTree.find_splits_bins(input, strategy)

    //TODO: Level-wise training of tree and obtain Decision Tree model


    return new DecisionTreeModel()
  }

}

object DecisionTree {
  def find_splits_bins(input : RDD[LabeledPoint], strategy : Strategy) : (Array[Array[Split]], Array[Array[Bin]]) = {
    val numSplits = strategy.numSplits
    //TODO: Justify this calculation
    val requiredSamples : Long = numSplits*numSplits
    val count : Long = input.count()
    val numSamples : Long = if (requiredSamples < count) requiredSamples else count
    val numFeatures = input.take(1)(0).features.length
    (Array.ofDim[Split](numFeatures,numSplits),Array.ofDim[Bin](numFeatures,numSplits))
  }

}