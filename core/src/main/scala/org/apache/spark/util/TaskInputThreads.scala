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

package org.apache.spark.util

import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.collect.MapMaker

import org.apache.spark.TaskContext

/**
 * Tells which tasks have a thread that reads the input of the task on its own, and that the task
 * does not wait for when it completes: the stdin writer thread of `PipedRDD`, the writer thread
 * of script transformation, and the writer thread of the R runner. Such a thread may still read
 * the batch that a reader of the task returned last while the completion listeners of the task
 * close that reader.
 *
 * So a reader whose batches are read at the addresses of their buffers, as the one of native
 * data sources, frees the buffers of that batch when it is closed only in a task without such a
 * thread, and otherwise once the batch is unreachable. An operator that reads the input of a
 * task on a thread that the task does not wait for registers it here before it starts the
 * thread. The writer of pipelined Python UDFs does not: the task waits for it before its
 * completion listeners run (`BasePythonRunner.startPipelinedWriter`).
 */
private[spark] object TaskInputThreads {
  // The flag of each task that was asked for. The keys are weak, compared by identity, so that
  // the flag of a task goes with its context. A task is the key rather than its attempt ID,
  // which is 0 for each context of `TaskContext.empty()`.
  private val unawaited = new MapMaker().weakKeys().makeMap[TaskContext, AtomicBoolean]()

  /**
   * Returns the flag that is set once a thread that the task of `context` does not wait for
   * reads its input. A reader keeps the flag rather than the context, which a batch that
   * outlives its task then does not keep alive.
   */
  def unawaitedFlag(context: TaskContext): AtomicBoolean =
    unawaited.computeIfAbsent(context, _ => new AtomicBoolean(false))

  /**
   * Tells that a thread that the task of `context` does not wait for reads its input, before
   * the thread starts. It does nothing without a task.
   */
  def registerUnawaited(context: TaskContext): Unit = {
    if (context != null) {
      unawaitedFlag(context).set(true)
    }
  }
}
