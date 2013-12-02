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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.model._
import org.apache.spark.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.Split


class DecisionTree(val strategy : Strategy) {

  def train(input : RDD[LabeledPoint]) : DecisionTreeModel = {

    //Cache input RDD for speedup during multiple passes
    input.cache()

    //TODO: Find all splits and bins using quantiles including support for categorical features, single-pass
    //TODO: Think about broadcasting this
    val (splits, bins) = DecisionTree.find_splits_bins(input, strategy)

    //TODO: Level-wise training of tree and obtain Decision Tree model

    val maxDepth = strategy.maxDepth

    val maxNumNodes = scala.math.pow(2,maxDepth).toInt - 1
    val filters = new Array[List[Filter]](maxNumNodes)

    for (level <- 0 until maxDepth){
      //Find best split for all nodes at a level
      val numNodes= scala.math.pow(2,level).toInt
      val bestSplits = DecisionTree.findBestSplits(input, strategy, level, filters,splits,bins)
      //TODO: update filters and decision tree model
    }

    return new DecisionTreeModel()
  }

}

object DecisionTree extends Logging {

  def findBestSplits(
                      input : RDD[LabeledPoint],
                      strategy: Strategy,
                      level: Int,
                      filters : Array[List[Filter]],
                      splits : Array[Array[Split]],
                      bins : Array[Array[Bin]]) : Array[Split] = {

    def findParentFilters(nodeIndex: Int): List[Filter] = {
      if (level == 0) {
        List[Filter]()
      } else {
        val nodeFilterIndex = scala.math.pow(2, level).toInt + nodeIndex
        val parentFilterIndex = nodeFilterIndex / 2
        filters(parentFilterIndex)
      }
    }

    def isSampleValid(parentFilters: List[Filter], labeledPoint: LabeledPoint): Boolean = {

      for (filter <- parentFilters) {
        val features = labeledPoint.features
        val featureIndex = filter.split.feature
        val threshold = filter.split.threshold
        val comparison = filter.comparison
        comparison match {
          case(-1) => if (features(featureIndex) > threshold) return false
          case(0) => if (features(featureIndex) != threshold) return false
          case(1) => if (features(featureIndex) <= threshold) return false
        }
      }
      true
    }

    def findBin(featureIndex: Int, labeledPoint: LabeledPoint) : Int = {

      //TODO: Do binary search
      for (binIndex <- 0 until strategy.numSplits) {
        val bin = bins(featureIndex)(binIndex)
        //TODO: Remove this requirement post basic functional testing
        require(bin.lowSplit.feature == featureIndex)
        require(bin.highSplit.feature == featureIndex)
        val lowThreshold = bin.lowSplit.threshold
        val highThreshold = bin.highSplit.threshold
        val features = labeledPoint.features
        if ((lowThreshold < features(featureIndex)) & (highThreshold < features(featureIndex))) {
          return binIndex
        }
      }
      throw new UnknownError("no bin was found.")

    }
    def findBinsForLevel: Array[Double] = {

      val numNodes = scala.math.pow(2, level).toInt
      //Find the number of features by looking at the first sample
      val numFeatures = input.take(1)(0).features.length

      //TODO: Bit pack more by removing redundant label storage
      // calculating bin index and label per feature per node
      val arr = new Array[Double](2 * numFeatures * numNodes)
      for (nodeIndex <- 0 until numNodes) {
        val parentFilters = findParentFilters(nodeIndex)
        //Find out whether the sample qualifies for the particular node
        val sampleValid = isSampleValid(parentFilters, labeledPoint)
        val shift = 2 * numFeatures * nodeIndex
        if (sampleValid) {
          //Add to invalid bin index -1
          for (featureIndex <- shift until (shift + numFeatures) by 2) {
            arr(featureIndex + 1) = -1
            arr(featureIndex + 2) = labeledPoint.label
          }
        } else {
          for (featureIndex <- 0 until numFeatures) {
            arr(shift + (featureIndex * 2) + 1) = findBin(featureIndex, labeledPoint)
            arr(shift + (featureIndex * 2) + 2) = labeledPoint.label
          }
        }

      }
      arr
    }

    val binMappedRDD = input.map(labeledPoint => findBinsForLevel)
    //calculate bin aggregates
    //find best split


    Array[Split]()
  }

  def find_splits_bins(input : RDD[LabeledPoint], strategy : Strategy) : (Array[Array[Split]], Array[Array[Bin]]) = {

    val numSplits = strategy.numSplits
    logDebug("numSplits = " + numSplits)

    //Calculate the number of sample for approximate quantile calculation
    //TODO: Justify this calculation
    val requiredSamples = numSplits*numSplits
    val count = input.count()
    val fraction = if (requiredSamples < count) requiredSamples.toDouble / count else 1.0
    logDebug("fraction of data used for calculating quantiles = " + fraction)

    //sampled input for RDD calculation
    val sampledInput = input.sample(false, fraction, 42).collect()
    val numSamples = sampledInput.length

    require(numSamples > numSplits, "length of input samples should be greater than numSplits")

    //Find the number of features by looking at the first sample
    val numFeatures = input.take(1)(0).features.length

    strategy.quantileCalculationStrategy match {
      case "sort" => {
        val splits =  Array.ofDim[Split](numFeatures,numSplits-1)
        val bins = Array.ofDim[Bin](numFeatures,numSplits)

        //Find all splits
        for (featureIndex <- 0 until numFeatures){
          val featureSamples  = sampledInput.map(lp => lp.features(featureIndex)).sorted
          val stride : Double = numSamples.toDouble/numSplits
          for (index <- 0 until numSplits-1) {
            val sampleIndex = (index+1)*stride.toInt
            val split = new Split(featureIndex,featureSamples(sampleIndex),"continuous")
            splits(featureIndex)(index) = split
          }
        }

        //Find all bins
        for (featureIndex <- 0 until numFeatures){
          bins(featureIndex)(0)
            = new Bin(new DummyLowSplit("continuous"),splits(featureIndex)(0),"continuous")
          for (index <- 1 until numSplits - 1){
            val bin = new Bin(splits(featureIndex)(index-1),splits(featureIndex)(index),"continuous")
            bins(featureIndex)(index) = bin
          }
          bins(featureIndex)(numSplits-1)
            = new Bin(splits(featureIndex)(numSplits-3),new DummyHighSplit("continuous"),"continuous")
        }

        (splits,bins)
      }
      case "minMax" => {
        (Array.ofDim[Split](numFeatures,numSplits),Array.ofDim[Bin](numFeatures,numSplits+2))
      }
      case "approximateHistogram" => {
        throw new UnsupportedOperationException("approximate histogram not supported yet.")
      }

    }
  }

}