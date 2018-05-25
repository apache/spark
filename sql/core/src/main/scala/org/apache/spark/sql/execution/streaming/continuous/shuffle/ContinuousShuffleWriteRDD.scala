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

package org.apache.spark.sql.execution.streaming.continuous.shuffle

import org.apache.spark.{Partition, Partitioner, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

class ContinuousShuffleWriteRDD(
    var prev: RDD[UnsafeRow],
    outputPartitioner: Partitioner,
    endpoints: Seq[RpcEndpointRef])
    extends RDD[Unit](prev) {

  override def getPartitions: Array[Partition] = prev.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Unit] = {
    val writer = new ContinuousShuffleWriter(split.index, outputPartitioner, endpoints)
    writer.write(prev.compute(split, context))

    Iterator()
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
