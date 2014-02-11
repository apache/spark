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

package org.apache.spark.rdd

import scala.reflect.ClassTag
import java.util.Random

import cern.jet.random.Poisson
import cern.jet.random.engine.DRand

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.util.random.BernoulliSampler

private[spark]
class FoldedRDDPartition(val prev: Partition, val seed: Int) extends Partition with Serializable {
  override val index: Int = prev.index
}

class FoldedRDD[T: ClassTag](
    prev: RDD[T],
    fold: Float,
    folds: Float,
    seed: Int)
  extends PartitionwiseSampledRDD[T, T](prev, new BernoulliSampler((fold-1)/folds,fold/folds, false), seed) {
}

/**
 * A companion class to FoldedRDD which contains all of the elements not in the fold for the same
 * fold/seed combination. Useful for cross validation
 */
class CompositeFoldedRDD[T: ClassTag](
    prev: RDD[T],
    fold: Float,
    folds: Float,
    seed: Int)
  extends PartitionwiseSampledRDD[T, T](prev, new BernoulliSampler((fold-1)/folds, fold/folds, true), seed) {
}
