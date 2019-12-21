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

package org.apache.spark.api.plugin;

import java.util.Map;

import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 * Executor component of a {@link SparkPlugin}.
 *
 * @since 3.0.0
 */
@DeveloperApi
public interface ExecutorPlugin {

  /**
   * Initialize the executor plugin.
   * <p>
   * When a Spark plugin provides an executor plugin, this method will be called during the
   * initialization of the executor process. It will block executor initialization until it
   * returns.
   * <p>
   * Executor plugins that publish metrics should register all metrics with the context's
   * registry ({@link PluginContext#metricRegistry()}) when this method is called. Metrics
   * registered afterwards are not guaranteed to show up.
   *
   * @param ctx Context information for the executor where the plugin is running.
   * @param extraConf Extra configuration provided by the driver component during its
   *                  initialization.
   */
  default void init(PluginContext ctx, Map<String, String> extraConf) {}

  /**
   * Clean up and terminate this plugin.
   * <p>
   * This method is called during the executor shutdown phase, and blocks executor shutdown.
   */
  default void shutdown() {}

}
