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

import org.apache.spark.BarrierTaskContext
import org.apache.spark.TaskContext
import org.apache.spark.annotation.{Experimental, Since}

/** Represents an RDD barrier, which forces Spark to launch tasks of this stage together. */
class RDDBarrier[T: ClassTag](rdd: RDD[T]) {

  /**
   * :: Experimental ::
   * Maps partitions together with a provided [[org.apache.spark.BarrierTaskContext]].
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless `rdd` is a pair RDD and the input function doesn't modify the keys.
   */
  @Experimental
  @Since("2.4.0")
  def mapPartitions[S: ClassTag](
      f: (Iterator[T], BarrierTaskContext) => Iterator[S],
      preservesPartitioning: Boolean = false): RDD[S] = rdd.withScope {
    val cleanedF = rdd.sparkContext.clean(f)
    new MapPartitionsRDD(
      rdd,
      (context: TaskContext, index: Int, iter: Iterator[T]) =>
        cleanedF(iter, context.asInstanceOf[BarrierTaskContext]),
      preservesPartitioning,
      isFromBarrier = true
    )
  }

  /** TODO extra conf(e.g. timeout) */
}
