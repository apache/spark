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

private[spark]
class FoldedRDDPartition(val prev: Partition, val seed: Int) extends Partition with Serializable {
  override val index: Int = prev.index
}

class FoldedRDD[T: ClassTag](
    prev: RDD[T],
    fold: Int,
    folds: Int,
    seed: Int)
  extends RDD[T](prev) {

  override def getPartitions: Array[Partition] = {
    val rg = new Random(seed)
    firstParent[T].partitions.map(x => new FoldedRDDPartition(x, rg.nextInt))
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[FoldedRDDPartition].prev)

  override def compute(splitIn: Partition, context: TaskContext): Iterator[T] = {
    val split = splitIn.asInstanceOf[FoldedRDDPartition]
    val rand = new Random(split.seed)
    firstParent[T].iterator(split.prev, context).filter(x => (rand.nextInt(folds) == fold-1))
  }
}

/**
 * A companion class to FoldedRDD which contains all of the elements not in the fold for the same
 * fold/seed combination. Useful for cross validation
 */
class CompositeFoldedRDD[T: ClassTag](
    prev: RDD[T],
    fold: Int,
    folds: Int,
    seed: Int)
  extends FoldedRDD[T](prev, fold, folds, seed) {

  override def compute(splitIn: Partition, context: TaskContext): Iterator[T] = {
    val split = splitIn.asInstanceOf[FoldedRDDPartition]
    val rand = new Random(split.seed)
    firstParent[T].iterator(split.prev, context).filter(x => (rand.nextInt(folds) != fold-1))
  }
}
