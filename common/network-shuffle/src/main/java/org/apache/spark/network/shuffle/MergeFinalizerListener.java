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

import org.apache.spark.network.shuffle.protocol.MergeStatuses;

/**
 * :: DeveloperApi ::
 *
 * Listener providing a callback function to invoke when driver receives the response for the
 * finalize shuffle merge request sent to remote shuffle service.
 *
 * @since 3.1.0
 */
public interface MergeFinalizerListener extends EventListener {
  /**
   * Called once upon successful response on finalize shuffle merge on a remote shuffle service.
   * The returned {@link MergeStatuses} is passed to the listener for further processing
   */
  void onShuffleMergeSuccess(MergeStatuses statuses);

  /**
   * Called once upon failure response on finalize shuffle merge on a remote shuffle service.
   */
  void onShuffleMergeFailure(Throwable e);
}
