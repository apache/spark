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
   * preferably performing expensive operations in a separate thread, or postponing them until
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
   * RPC message handler.
   * <p>
   * Plugins can use Spark's RPC system to send messages from executors to the driver (but not
   * the other way around, currently). Messages sent by the executor component of the plugin will
   * be delivered to this method, and the returned value will be sent back to the executor as
   * the reply, if the executor has requested one.
   * <p>
   * Any exception thrown will be sent back to the executor as an error, in case it is expecting
   * a reply. In case a reply is not expected, a log message will be written to the driver log.
   * <p>
   * The implementation of this handler should be thread-safe.
   * <p>
   * Note all plugins share RPC dispatch threads, and this method is called synchronously. So
   * performing expensive operations in this handler may affect the operation of other active
   * plugins. Internal Spark endpoints are not directly affected, though, since they use different
   * threads.
   * <p>
   * Spark guarantees that the driver component will be ready to receive messages through this
   * handler when executors are started.
   *
   * @param message The incoming message.
   * @return Value to be returned to the caller. Ignored if the caller does not expect a reply.
   */
  default Object receive(Object message) throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Informs the plugin that the Spark application is shutting down.
   * <p>
   * This method is called during the driver shutdown phase. It is recommended that plugins
   * not use any Spark functions (e.g. send RPC messages) during this call.
   */
  default void shutdown() {}

}
