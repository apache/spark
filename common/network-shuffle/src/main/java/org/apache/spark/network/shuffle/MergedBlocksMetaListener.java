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

package org.apache.spark.network.shuffle;

import java.util.EventListener;

/**
 * Listener for receiving success or failure events when fetching meta of merged blocks.
 *
 * @since 3.2.0
 */
public interface MergedBlocksMetaListener extends EventListener {

  /**
   * Called after successfully receiving the meta of a merged block.
   *
   * @param shuffleId shuffle id.
   * @param shuffleMergeId shuffleMergeId is used to uniquely identify merging process
   *                       of shuffle by an indeterminate stage attempt.
   * @param reduceId reduce id.
   * @param meta contains meta information of a merged block.
   */
  void onSuccess(int shuffleId, int shuffleMergeId, int reduceId, MergedBlockMeta meta);

  /**
   * Called when there is an exception while fetching the meta of a merged block.
   *
   * @param shuffleId shuffle id.
   * @param shuffleMergeId shuffleMergeId is used to uniquely identify merging process
   *                       of shuffle by an indeterminate stage attempt.
   * @param reduceId reduce id.
   * @param exception exception getting chunk counts.
   */
  void onFailure(int shuffleId, int shuffleMergeId, int reduceId, Throwable exception);
}
