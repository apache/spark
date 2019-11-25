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

import java.util.Collections;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 * Driver component of a {@link SparkPlugin}.
 *
 * @since 3.0.0
 */
@DeveloperApi
public interface DriverPlugin {

  /**
   * Initialize the plugin.
   * <p>
   * This method is called early in the initialization of the Spark driver. Explicitly, it is
   * called before the Spark driver's task scheduler is initialized. This means that a lot
   * of other Spark subsystems may yet not have been initialized. This call also blocks driver
   * initialization.
   * <p>
   * It's recommended that plugins be careful about what operations are performed in this call,
   * preferrably performing expensive operations in a separate thread, or postponing them until
   * the application has fully started.
   *
   * @param sc The SparkContext loading the plugin.
   * @param pluginContext Additional plugin-specific about the Spark application where the plugin
   *                      is running.
   * @return A map that will be provided to the {@link ExecutorPlugin#init(PluginContext,Map)}
   *         method.
   */
  default Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
    return Collections.emptyMap();
  }

  /**
   * Register metrics published by the plugin with Spark's metrics system.
   * <p>
   * This method is called later in the initialization of the Spark application, after most
   * subsystems are up and the application ID is known. If there are metrics registered in
   * the registry ({@link PluginContext#metricRegistry()}), then a metrics source with the
   * plugin name will be created.
   * <p>
   * Note that even though the metric registry is still accessible after this method is called,
   * registering new metrics after this method is called may result in the metrics not being
   * available.
   *
   * @param appId The application ID from the cluster manager.
   * @param pluginContext Additional plugin-specific about the Spark application where the plugin
   *                      is running.
   */
  default void registerMetrics(String appId, PluginContext pluginContext) {}

  /**
   * RPC endpoint for the plugin. The default implementation checks whether the implementing class
   * is an instance of {@link PluginRpcEndpoint}, and returns itself in that case. Otherwise
   * it returns <code>null</code>
   *
   * @return The RPC endpoint, or <code>null</code> if the plugin does not need one.
   */
  default PluginRpcEndpoint rpcEndpoint() {
    return (this instanceof PluginRpcEndpoint) ? ((PluginRpcEndpoint) this) : null;
  }

  /**
   * Informs the plugin that the Spark application is shutting down.
   * <p>
   * This method is called during the driver shutdown phase. It is recommended that plugins
   * not use any Spark functions (e.g. send RPC messages) during this call.
   */
  default void shutdown() {}

}
