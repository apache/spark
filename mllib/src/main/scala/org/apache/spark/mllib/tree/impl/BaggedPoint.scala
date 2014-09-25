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

import cern.jet.random.Poisson
import cern.jet.random.engine.DRand

import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * Internal representation of a datapoint which belongs to several subsamples of the same dataset,
 * particularly for bagging (e.g., for random forests).
 *
 * This holds one instance, as well as an array of weights which represent the (weighted)
 * number of times which this instance appears in each subsample.
 * E.g., (datum, [1, 0, 4]) indicates that there are 3 subsamples of the dataset and that
 * this datum has 1 copy, 0 copies, and 4 copies in the 3 subsamples, respectively.
 *
 * @param datum  Data instance
 * @param subsampleWeights  Weight of this instance in each subsampled dataset.
 *
 * TODO: This does not currently support (Double) weighted instances.  Once MLlib has weighted
 *       dataset support, update.  (We store subsampleWeights as Double for this future extension.)
 */
private[tree] class BaggedPoint[Datum](val datum: Datum, val subsampleWeights: Array[Double])
  extends Serializable

private[tree] object BaggedPoint {

  /**
   * Convert an input dataset into its BaggedPoint representation,
   * choosing subsample counts for each instance.
   * Each subsample has the same number of instances as the original dataset,
   * and is created by subsampling with replacement.
   * @param input     Input dataset.
   * @param numSubsamples  Number of subsamples of this RDD to take.
   * @param seed   Random seed.
   * @return  BaggedPoint dataset representation
   */
  def convertToBaggedRDD[Datum](
      input: RDD[Datum],
      numSubsamples: Int,
      seed: Int = Utils.random.nextInt()): RDD[BaggedPoint[Datum]] = {
    input.mapPartitionsWithIndex { (partitionIndex, instances) =>
      // TODO: Support different sampling rates, and sampling without replacement.
      // Use random seed = seed + partitionIndex + 1 to make generation reproducible.
      val poisson = new Poisson(1.0, new DRand(seed + partitionIndex + 1))
      instances.map { instance =>
        val subsampleWeights = new Array[Double](numSubsamples)
        var subsampleIndex = 0
        while (subsampleIndex < numSubsamples) {
          subsampleWeights(subsampleIndex) = poisson.nextInt()
          subsampleIndex += 1
        }
        new BaggedPoint(instance, subsampleWeights)
      }
    }
  }

  def convertToBaggedRDDWithoutSampling[Datum](input: RDD[Datum]): RDD[BaggedPoint[Datum]] = {
    input.map(datum => new BaggedPoint(datum, Array(1.0)))
  }

}
