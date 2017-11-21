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

import org.apache.spark._
import org.apache.spark.rdd.RDD

private[columnar] class FilteredCachedColumnarPartition(
    val partitionIndex: Int,
    val parentPartitionIndex: Int) extends Partition {

  override def index: Int = partitionIndex
}

private class PartialDependency[T](rdd: RDD[T], partitions: Array[Partition])
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int): Seq[Int] = {
    List(partitions(partitionId).asInstanceOf[FilteredCachedColumnarPartition].parentPartitionIndex)
  }
}

private[columnar] class FilteredCachedColumnarRDD (
    @transient private var _sc: SparkContext,
    private var cachedColumnarRDD: CachedColumnarRDD,
    partitions: Seq[Partition])
  extends RDD[CachedBatch](_sc, Seq(new PartialDependency(cachedColumnarRDD, partitions.toArray))) {

  override def compute(split: Partition, context: TaskContext): Iterator[CachedBatch] = {
    firstParent.iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = cachedColumnarRDD.partitions

}
