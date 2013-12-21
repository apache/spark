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

package org.apache.spark.api.r

import org.apache.spark.Partitioner
import java.util.Arrays
import org.apache.spark.util.Utils

/**
 * A [[org.apache.spark.Partitioner]] that performs handling of byte arrays, for use by the R API.
 */
class RPartitioner(
  override val numPartitions: Int,
  val rPartitionFunctionId: String)
  extends Partitioner {

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case key: Array[Byte] => Utils.nonNegativeMod(Arrays.hashCode(key), numPartitions)
    case _ => Utils.nonNegativeMod(key.hashCode(), numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: RPartitioner =>
      h.numPartitions == numPartitions && h.rPartitionFunctionId == rPartitionFunctionId
    case _ => false
  }
}

