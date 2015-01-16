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

package org.apache.spark.api.python

import org.apache.spark.Partitioner
import org.apache.spark.util.Utils

/**
 * A [[org.apache.spark.Partitioner]] that performs handling of long-valued keys, for use by the
 * Python API.
 *
 * Stores the unique id() of the Python-side partitioning function so that it is incorporated into
 * equality comparisons.  Correctness requires that the id is a unique identifier for the
 * lifetime of the program (i.e. that it is not re-used as the id of a different partitioning
 * function).  This can be ensured by using the Python id() function and maintaining a reference
 * to the Python partitioning function so that its id() is not reused.
 */

private[spark] class PythonPartitioner(
  var partitions: Int,
  val pyPartitionFunctionId: Long)
  extends Partitioner {

  override def numPartitions = partitions

  override def getPartition(key: Any): Int = key match {
    case null => 0
    // we don't trust the Python partition function to return valid partition ID's so
    // let's do a modulo numPartitions in any case
    case key: Long => Utils.nonNegativeMod(key.toInt, numPartitions)
    case _ => Utils.nonNegativeMod(key.hashCode(), numPartitions)
  }

  override def setNumPartitions(numPartitions: Int) = {
    partitions = numPartitions
  }

  override def equals(other: Any): Boolean = other match {
    case h: PythonPartitioner =>
      h.numPartitions == numPartitions && h.pyPartitionFunctionId == pyPartitionFunctionId
    case _ =>
      false
  }

  override def hashCode: Int = 31 * numPartitions + pyPartitionFunctionId.hashCode
}
