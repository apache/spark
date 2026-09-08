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

import org.apache.spark.annotation.DeveloperApi

/**
 * An opaque handle to a shuffle, used by a ShuffleManager to pass information about it to tasks.
 *
 * @param shuffleId ID of the shuffle
 */
@DeveloperApi
abstract class ShuffleHandle(val shuffleId: Int) extends Serializable {
  /**
   * Whether this shuffle's output is stored reliably, independent of the lifecycle of the executor
   * host that produced it (e.g. a remote shuffle service or a distributed filesystem), matching the
   * contract of `ShuffleDriverComponents.supportsReliableStorage()`. When true, losing the executor
   * or its host does not lose this shuffle's output, so its map outputs are not unregistered on
   * executor/worker loss.
   *
   * This is the per-shuffle override of the app-global `supportsReliableStorage()`: `Some(value)`
   * is authoritative for this shuffle (a ShuffleManager that routes reliability per shuffle sets it
   * on the handle it returns), while `None` means "no per-shuffle information", in which case the
   * global flag is used. Defaults to `None` so managers that don't set it keep the global behavior.
   */
  def reliablyStored: Option[Boolean] = None
}
