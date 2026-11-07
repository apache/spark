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

package org.apache.spark.shuffle

import java.util.Optional

import org.apache.spark.annotation.DeveloperApi

/**
 * An opaque handle to a shuffle, used by a ShuffleManager to pass information about it to tasks.
 *
 * @param shuffleId ID of the shuffle
 */
@DeveloperApi
abstract class ShuffleHandle(val shuffleId: Int) extends Serializable {
  /**
   * Whether this shuffle's output is stored reliably, off the executor that produced it (e.g. a
   * remote shuffle service or distributed filesystem). When true, losing the executor or its host
   * does not lose the output, so its map outputs are not unregistered on executor/worker loss.
   *
   * Applies only to materialized, map-output-tracked (blocking) shuffles. A pipelined shuffle is
   * located through the streaming task-location registry, not map-status cleanup, so it cannot
   * honor this flag; the scheduler rejects a non-`None` value on a pipelined dependency.
   *
   * Per-shuffle override of the app-global `ShuffleDriverComponents.supportsReliableStorage()`:
   * a present value is authoritative for this shuffle. Empty (the default) means "no per-shuffle
   * information", so the shuffle takes the global flag. A manager that sets neither keeps today's
   * behavior. With the global flag on, an empty value marks the shuffle reliable, so its output is
   * also kept on worker loss and on a FetchFailed for another shuffle.
   */
  def reliablyStored: Optional[java.lang.Boolean] = Optional.empty()
}
