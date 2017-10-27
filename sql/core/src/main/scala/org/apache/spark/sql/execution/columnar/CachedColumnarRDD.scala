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

package org.apache.spark.sql.execution.columnar

import scala.reflect.ClassTag

import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow

private[columnar] class CachedColumnarRDDPartition(
    partitionIndex: Int,
    columnnStats: Array[InternalRow]) extends Partition {

  override def index: Int = partitionIndex

  def columnStats: Array[InternalRow] = columnnStats
}

private[columnar] class CachedColumnarRDD[T: ClassTag](
    @transient private var _sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]],
    dataRDD: RDD[T],
    partitionStats: Array[Array[InternalRow]]) extends RDD[T](_sc, deps) {

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    Iterator()
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   *
   * The partitions in this array must satisfy the following property:
   * `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
   */
  override protected def getPartitions: Array[Partition] = {
    partitionStats.zipWithIndex.map {
      case (statsRow, index) =>
        new CachedColumnarRDDPartition(index, statsRow)
    }
  }
}
