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

/**
 * Implemented by an RDD that reads a shuffle, to declare how ITS partition index maps to the
 * shuffle's REDUCE partition index.
 *
 * The scheduler needs this for a pipelined shuffle with a partial read: it must tell the producer
 * which reduce partitions actually have a reader, and it derives that from the subset of result
 * partitions the job runs (see `DAGScheduler.liveReduceSet`). Equal partition COUNTS do not imply
 * an identity mapping -- a reader that splits one reduce partition across two of its partitions and
 * coalesces two others into one has the same count and a different mapping -- and guessing wrong is
 * not a hang but silently dropped records, since the producer skips every partition outside the set
 * it is given. So the mapping is asked of the reader rather than inferred.
 *
 * An implementation returns `None` for a partition whose reduce index is not a single well-defined
 * value (a coalesced range, a partial-mapper split); the scheduler then treats the mapping as
 * uncomputable and keeps every reduce partition live.
 */
private[spark] trait ShuffleReducePartitionMapping {
  /**
   * The single reduce partition index this RDD's partition `partitionIndex` reads, or `None` if it
   * reads no single reduce partition (e.g. a coalesced multi-reducer range).
   */
  def reducePartitionIndex(partitionIndex: Int): Option[Int]
}
