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

package org.apache.spark.mllib.tree.impl

import scala.collection.mutable


/**
 * :: Experimental ::
 * DecisionTree statistics aggregator.
 * This holds a flat array of statistics for a set of (nodes, features, bins)
 * and helps with indexing.
 * TODO: Allow views of Vector types to replace some of the code in here.
 */
private[tree] class DTStatsAggregator(
    val numNodes: Int,
    val numFeatures: Int,
    val numBins: Array[Int],
    val statsSize: Int) {

  require(numBins.size == numFeatures, s"DTStatsAggregator was given numBins" +
    s" (of size ${numBins.size}) which did not match numFeatures = $numFeatures.")

  val featureOffsets: Array[Int] = numBins.scanLeft(0)(_ + _).map(statsSize * _)

  val allStatsSize: Int = numNodes * featureOffsets.last * statsSize

  val allStats: Array[Double] = new Array[Double](allStatsSize)

// TODO: Make views
  /*
  Uses:
  point access

   */

}
