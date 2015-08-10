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

package org.apache.spark.ui

private[spark] object ToolTips {
  val SCHEDULER_DELAY =
    """Scheduler delay includes time to ship the task from the scheduler to
       the executor, and time to send the task result from the executor to the scheduler. If
       scheduler delay is large, consider decreasing the size of tasks or decreasing the size
       of task results."""

  val TASK_DESERIALIZATION_TIME =
    """Time spent deserializing the task closure on the executor, including the time to read the
       broadcasted task."""

  val SHUFFLE_READ_BLOCKED_TIME =
    "Time that the task spent blocked waiting for shuffle data to be read from remote machines."

  val INPUT = "Bytes and records read from Hadoop or from Spark storage."

  val OUTPUT = "Bytes and records written to Hadoop."

  val STORAGE_MEMORY =
    "Memory used / total available memory for storage of data " +
      "like RDD partitions cached in memory. "

  val SHUFFLE_WRITE =
    "Bytes and records written to disk in order to be read by a shuffle in a future stage."

  val SHUFFLE_READ =
    """Total shuffle bytes and records read (includes both data read locally and data read from
       remote executors). """

  val SHUFFLE_READ_REMOTE_SIZE =
    """Total shuffle bytes read from remote executors. This is a subset of the shuffle
       read bytes; the remaining shuffle data is read locally. """

  val GETTING_RESULT_TIME =
    """Time that the driver spends fetching task results from workers. If this is large, consider
       decreasing the amount of data returned from each task."""

  val RESULT_SERIALIZATION_TIME =
    """Time spent serializing the task result on the executor before sending it back to the
       driver."""

  val GC_TIME =
    """Time that the executor spent paused for Java garbage collection while the task was
       running."""

  val PEAK_EXECUTION_MEMORY =
    """Execution memory refers to the memory used by internal data structures created during
       shuffles, aggregations and joins when Tungsten is enabled. The value of this accumulator
       should be approximately the sum of the peak sizes across all such data structures created
       in this task. For SQL jobs, this only tracks all unsafe operators, broadcast joins, and
       external sort."""

  val JOB_TIMELINE =
    """Shows when jobs started and ended and when executors joined or left. Drag to scroll.
       Click Enable Zooming and use mouse wheel to zoom in/out."""

  val STAGE_TIMELINE =
    """Shows when stages started and ended and when executors joined or left. Drag to scroll.
       Click Enable Zooming and use mouse wheel to zoom in/out."""

  val JOB_DAG =
    """Shows a graph of stages executed for this job, each of which can contain
       multiple RDD operations (e.g. map() and filter()), and of RDDs inside each operation
       (shown as dots)."""

  val STAGE_DAG =
    """Shows a graph of RDD operations in this stage, and RDDs inside each one. A stage can run
       multiple operations (e.g. two map() functions) if they can be pipelined. Some operations
       also create multiple RDDs internally. Cached RDDs are shown in green.
    """
}
