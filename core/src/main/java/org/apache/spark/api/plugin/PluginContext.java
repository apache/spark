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

import java.io.IOException;
import java.util.Map;

import com.codahale.metrics.MetricRegistry;

import org.apache.spark.SparkConf;
import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.resource.ResourceInformation;

/**
 * :: DeveloperApi ::
 * Context information and operations for plugins loaded by Spark.
 * <p>
 * An instance of this class is provided to plugins in their initialization method. It is safe
 * for plugins to keep a reference to the instance for later use (for example, to send messages
 * to the plugin's driver component).
 * <p>
 * Context instances are plugin-specific, so metrics and messages are tied each plugin. It is
 * not possible for a plugin to directly interact with other plugins.
 *
 * @since 3.0.0
 */
@DeveloperApi
public interface PluginContext {

  /**
   * Registry where to register metrics published by the plugin associated with this context.
   */
  MetricRegistry metricRegistry();

  /** Configuration of the Spark application. */
  SparkConf conf();

  /** Executor ID of the process. On the driver, this will identify the driver. */
  String executorID();

  /** The host name which is being used by the Spark process for communication. */
  String hostname();

  /** The custom resources (GPUs, FPGAs, etc) allocated to driver or executor. */
  Map<String, ResourceInformation> resources();

  /**
   * Send a message to the plugin's driver-side component.
   * <p>
   * This method sends a message to the driver-side component of the plugin, without expecting
   * a reply. It returns as soon as the message is enqueued for sending.
   * <p>
   * The message must be serializable.
   *
   * @param message Message to be sent.
   */
  void send(Object message) throws IOException;

  /**
   * Send an RPC to the plugin's driver-side component.
   * <p>
   * This method sends a message to the driver-side component of the plugin, and blocks until a
   * reply arrives, or the configured RPC ask timeout (<code>spark.rpc.askTimeout</code>) elapses.
   * <p>
   * If the driver replies with an error, an exception with the corresponding error will be thrown.
   * <p>
   * The message must be serializable.
   *
   * @param message Message to be sent.
   * @return The reply from the driver-side component.
   */
  Object ask(Object message) throws Exception;

}
