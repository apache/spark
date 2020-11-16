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

import org.apache.spark.TaskContext
import org.apache.spark.annotation.{Experimental, Since}

/**
 * :: Experimental ::
 * Wraps an RDD in a barrier stage, which forces Spark to launch tasks of this stage together.
 * [[org.apache.spark.rdd.RDDBarrier]] instances are created by
 * [[org.apache.spark.rdd.RDD#barrier]].
 */
@Experimental
@Since("2.4.0")
class RDDBarrier[T: ClassTag] private[spark] (rdd: RDD[T]) {

  /**
   * :: Experimental ::
   * Returns a new RDD by applying a function to each partition of the wrapped RDD,
   * where tasks are launched together in a barrier stage.
   * The interface is the same as [[org.apache.spark.rdd.RDD#mapPartitions]].
   * Please see the API doc there.
   * @see [[org.apache.spark.BarrierTaskContext]]
   */
  @Experimental
  @Since("2.4.0")
  def mapPartitions[S: ClassTag](
      f: Iterator[T] => Iterator[S],
      preservesPartitioning: Boolean = false): RDD[S] = rdd.withScope {
    val cleanedF = rdd.sparkContext.clean(f)
    new MapPartitionsRDD(
      rdd,
      (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(iter),
      preservesPartitioning,
      isFromBarrier = true
    )
  }

  /**
   * :: Experimental ::
   * Returns a new RDD by applying a function to each partition of the wrapped RDD, while tracking
   * the index of the original partition. And all tasks are launched together in a barrier stage.
   * The interface is the same as [[org.apache.spark.rdd.RDD#mapPartitionsWithIndex]].
   * Please see the API doc there.
   * @see [[org.apache.spark.BarrierTaskContext]]
   */
  @Experimental
  @Since("3.0.0")
  def mapPartitionsWithIndex[S: ClassTag](
      f: (Int, Iterator[T]) => Iterator[S],
      preservesPartitioning: Boolean = false): RDD[S] = rdd.withScope {
    val cleanedF = rdd.sparkContext.clean(f)
    new MapPartitionsRDD(
      rdd,
      (_: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(index, iter),
      preservesPartitioning,
      isFromBarrier = true
    )
  }

  // TODO: [SPARK-25247] add extra conf to RDDBarrier, e.g., timeout.
}
