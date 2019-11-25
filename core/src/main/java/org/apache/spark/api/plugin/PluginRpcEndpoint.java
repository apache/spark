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
 * RPC message handler for Spark driver plugins.
 * <p>
 * Plugins can use Spark's RPC system to send messages from executors to the driver (but not
 * the other way around, currently). Messages sent by the executor component of the plugin will
 * be delivered to the {@link #receive(Object)} method.
 * <p>
 * Spark guarantees that the driver component will be ready to receive messages through this
 * handler when executors are started.
 *
 * @since 3.0.0
 */
@DeveloperApi
public interface PluginRpcEndpoint {

  /**
   * Handle an RPC message sent by the plugin's executor component.
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
   *
   * @param message The incoming message.
   * @return Value to be returned to the caller. Ignored if the caller does not expect a reply.
   */
  Object receive(Object message) throws Exception;

}
