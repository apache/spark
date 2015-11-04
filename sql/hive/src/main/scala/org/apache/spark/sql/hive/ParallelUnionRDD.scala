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

package org.apache.spark.sql.hive

import java.util.concurrent.Callable

import org.apache.spark.rdd.{RDD, UnionPartition, UnionRDD}
import org.apache.spark.util.ThreadUtils
import org.apache.spark.{Partition, SparkContext}

import scala.reflect.ClassTag

class ParallelUnionRDD[T: ClassTag](
  sc: SparkContext,
  rdds: Seq[RDD[T]]) extends UnionRDD[T](sc, rdds){
  // TODO: We might need to guess a more reasonable thread pool size here
  @transient val executorService = ThreadUtils.newDaemonFixedThreadPool(
    Math.min(rdds.size, Runtime.getRuntime.availableProcessors()), "ParallelUnionRDD")

  override def getPartitions: Array[Partition] = {
    // Calc partitions field for each RDD in parallel.
    val rddPartitions = rdds.map {rdd =>
      (rdd, executorService.submit(new Callable[Array[Partition]] {
        override def call(): Array[Partition] = rdd.partitions
      }))
    }.map {case(r, f) => (r, f.get())}

    val array = new Array[Partition](rddPartitions.map(_._2.length).sum)
    var pos = 0
    for (((rdd, partitions), rddIndex) <- rddPartitions.zipWithIndex; split <- partitions) {
      array(pos) = new UnionPartition(pos, rdd, rddIndex, split.index)
      pos += 1
    }
    array
  }

}
