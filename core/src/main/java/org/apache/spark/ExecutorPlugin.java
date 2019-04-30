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

package org.apache.spark;

import org.apache.spark.annotation.DeveloperApi;

/**
 * A plugin which can be automatically instantiated within each Spark executor.  Users can specify
 * plugins which should be created with the "spark.executor.plugins" configuration.  An instance
 * of each plugin will be created for every executor, including those created by dynamic allocation,
 * before the executor starts running any tasks.
 *
 * The specific api exposed to the end users still considered to be very unstable.  We will
 * hopefully be able to keep compatibility by providing default implementations for any methods
 * added, but make no guarantees this will always be possible across all Spark releases.
 *
 * Spark does nothing to verify the plugin is doing legitimate things, or to manage the resources
 * it uses.  A plugin acquires the same privileges as the user running the task.  A bad plugin
 * could also interfere with task execution and make the executor fail in unexpected ways.
 */
@DeveloperApi
public interface ExecutorPlugin {

  /**
   * Initialize the executor plugin.
   *
   * <p>Each executor will, during its initialization, invoke this method on each
   * plugin provided in the spark.executor.plugins configuration.</p>
   *
   * <p>Plugins should create threads in their implementation of this method for
   * any polling, blocking, or intensive computation.</p>
   */
  default void init() {}

  /**
   * Clean up and terminate this plugin.
   *
   * <p>This function is called during the executor shutdown phase. The executor
   * will wait for the plugin to terminate before continuing its own shutdown.</p>
   */
  default void shutdown() {}
}
