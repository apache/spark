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

package org.apache.spark.shuffle.api;

import org.apache.spark.annotation.Private;

/**
 * :: Private ::
 * An interface for building shuffle support for Executors.
 *
 * @since 3.0.0
 */
@Private
public interface ShuffleExecutorComponents {

  /**
   * Called once per executor to bootstrap this module with state that is specific to
   * that executor, specifically the application ID and executor ID.
   */
  void initializeExecutor(String appId, String execId);

  /**
   * Returns the modules that are responsible for persisting shuffle data to the backing
   * store.
   * <p>
   * This may be called multiple times on each executor. Implementations should not make
   * any assumptions about the lifetime of the returned module.
   */
  ShuffleWriteSupport writes();
}
