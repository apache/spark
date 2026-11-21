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
   * Whether this shuffle's output survives losing the executor or host that wrote it (e.g. on a
   * remote shuffle service). If so, executor or worker loss keeps its map output registered.
   *
   * Overrides `ShuffleDriverComponents.supportsReliableStorage()` for this shuffle. Empty (the
   * default) falls back to that flag. The driver reads it once, when it registers the shuffle.
   * A pipelined shuffle must leave it empty.
   *
   * TODO(SPARK-59806): dynamic allocation (`ExecutorAllocationManager`) still reads only the
   * global flag.
   */
  def reliablyStored: Optional[java.lang.Boolean] = Optional.empty()
}
