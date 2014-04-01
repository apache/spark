/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util;

import java.io.Closeable;

/**
 * Service plug-in interface.
 * 
 * Service plug-ins may be used to expose functionality of datanodes or
 * namenodes using arbitrary RPC protocols. Plug-ins are instantiated by the
 * service instance, and are notified of service life-cycle events using the
 * methods defined by this class.
 * 
 * Service plug-ins are started after the service instance is started, and
 * stopped before the service instance is stopped.
 */
public interface ServicePlugin extends Closeable {

  /**
   * This method is invoked when the service instance has been started.
   *
   * If the plugin fails to initialize and throws an exception, the
   * PluginDispatcher instance will log an error and remove the plugin.
   *
   * @param service The service instance invoking this method
   */
  void start(Object service);
  
  /**
   * This method is invoked when the service instance is about to be shut down.
   */
  void stop();
}
