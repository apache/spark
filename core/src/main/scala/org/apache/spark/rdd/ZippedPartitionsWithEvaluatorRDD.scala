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

import org.apache.spark.{Partition, PartitionEvaluatorFactory, TaskContext}

private[spark] class ZippedPartitionsWithEvaluatorRDD[T : ClassTag, U : ClassTag](
    var rdd1: RDD[T],
    var rdd2: RDD[T],
    evaluatorFactory: PartitionEvaluatorFactory[T, U])
  extends ZippedPartitionsBaseRDD[U](rdd1.context, List(rdd1, rdd2)) {

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    val evaluator = evaluatorFactory.createEvaluator()
    val partitions = split.asInstanceOf[ZippedPartitionsPartition].partitions
    evaluator.eval(
      split.index,
      rdd1.iterator(partitions(0), context),
      rdd2.iterator(partitions(1), context))
  }
}
