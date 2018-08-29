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

package org.apache.spark.scheduler

import org.apache.spark.SparkException

/**
 * Exception thrown when submit a job with barrier stage(s) failing a required check.
 */
private[spark] class BarrierJobAllocationFailed(message: String) extends SparkException(message)

private[spark] class BarrierJobUnsupportedRDDChainException
  extends BarrierJobAllocationFailed(
    BarrierJobAllocationFailed.ERROR_MESSAGE_RUN_BARRIER_WITH_UNSUPPORTED_RDD_CHAIN_PATTERN)

private[spark] class BarrierJobRunWithDynamicAllocationException
  extends BarrierJobAllocationFailed(
    BarrierJobAllocationFailed.ERROR_MESSAGE_RUN_BARRIER_WITH_DYN_ALLOCATION)

private[spark] class BarrierJobSlotsNumberCheckFailed
  extends BarrierJobAllocationFailed(
    BarrierJobAllocationFailed.ERROR_MESSAGE_BARRIER_REQUIRE_MORE_SLOTS_THAN_CURRENT_TOTAL_NUMBER)

private[spark] object BarrierJobAllocationFailed {

  // Error message when running a barrier stage that have unsupported RDD chain pattern.
  val ERROR_MESSAGE_RUN_BARRIER_WITH_UNSUPPORTED_RDD_CHAIN_PATTERN =
    "[SPARK-24820][SPARK-24821]: Barrier execution mode does not allow the following pattern of " +
      "RDD chain within a barrier stage:\n1. Ancestor RDDs that have different number of " +
      "partitions from the resulting RDD (eg. union()/coalesce()/first()/take()/" +
      "PartitionPruningRDD). A workaround for first()/take() can be barrierRdd.collect().head " +
      "(scala) or barrierRdd.collect()[0] (python).\n" +
      "2. An RDD that depends on multiple barrier RDDs (eg. barrierRdd1.zip(barrierRdd2))."

  // Error message when running a barrier stage with dynamic resource allocation enabled.
  val ERROR_MESSAGE_RUN_BARRIER_WITH_DYN_ALLOCATION =
    "[SPARK-24942]: Barrier execution mode does not support dynamic resource allocation for " +
      "now. You can disable dynamic resource allocation by setting Spark conf " +
      "\"spark.dynamicAllocation.enabled\" to \"false\"."

  // Error message when running a barrier stage that requires more slots than current total number.
  val ERROR_MESSAGE_BARRIER_REQUIRE_MORE_SLOTS_THAN_CURRENT_TOTAL_NUMBER =
    "[SPARK-24819]: Barrier execution mode does not allow run a barrier stage that requires " +
      "more slots than the total number of slots in the cluster currently. Please init a new " +
      "cluster with more CPU cores or repartition the input RDD(s) to reduce the number of " +
      "slots required to run this barrier stage."
}
