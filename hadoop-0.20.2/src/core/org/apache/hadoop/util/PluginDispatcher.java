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

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Provides convenience functions for dispatching calls through
 * to plugins registered with a class. Classes that wish to provide
 * plugin interfaces should use this class to load the plugin list
 * from the Configuration and to dispatch calls to the loaded instances.
 *
 * Calls dispatched through this class are performed on a second thread
 * so as to not block execution of the plugged service.
 */
public class PluginDispatcher<T extends ServicePlugin> {
  public static final Log LOG = LogFactory.getLog(PluginDispatcher.class.getName());

  private final List<T> plugins;
  private Executor executor;

  /**
   * Load a PluginDispatcher from the given Configuration. The start()
   * callback will not be automatically called.
   *
   * @param conf the Configuration from which to load
   * @param key the configuration key that lists class names to instantiate
   * @param clazz the class or interface from which plugins must extend
   */
  public static <X extends ServicePlugin> PluginDispatcher<X> createFromConfiguration(
    Configuration conf, String key, Class<X> clazz) {
    List<X> plugins = new ArrayList<X>();
    try {
      plugins.addAll(conf.getInstances(key, clazz));
    } catch (Throwable t) {
      LOG.warn("Unable to load "+key+" plugins");
    }
    return new PluginDispatcher<X>(plugins);
  }

  PluginDispatcher(Collection<T> plugins) {
    this.plugins = Collections.synchronizedList(new ArrayList<T>(plugins));
    executor = Executors.newSingleThreadExecutor();
  }

  PluginDispatcher(Collection<T> plugins, Executor executor) {
    this.plugins = Collections.synchronizedList(new ArrayList<T>(plugins));
    this.executor = executor;
  }

  /**
   * Dispatch a call to all active plugins.
   *
   * Exceptions will be caught and logged at WARN level.
   *
   * @param callback a function which will run once for each plugin, with
   * that plugin as the argument
   */
  public void dispatchCall(final SingleArgumentRunnable<T> callback) {
    executor.execute(new Runnable() {
      public void run() {
        for (T plugin : plugins) {
          try {
            callback.run(plugin);
          } catch (Throwable t) {
            LOG.warn("Uncaught exception dispatching to plugin " + plugin, t);
          }
        }
      }});
  }

  /**
   * Dispatches the start(...) hook common to all ServicePlugins. This
   * also automatically removes any plugin that throws an exception while
   * attempting to start.
   *
   * @param plugPoint passed to ServicePlugin.start()
   */
  public void dispatchStart(final Object plugPoint) {
    dispatchCall(
      new SingleArgumentRunnable<T>() {
        public void run(T p) {
          try {
            p.start(plugPoint);
          } catch (Throwable t) {
            LOG.error("ServicePlugin " + p + " could not be started. " +
                      "Removing from future callbacks.", t);
            plugins.remove(p);
          }
        }
      });
  }

  /**
   * Convenience function for dispatching the stop() hook common to all
   * ServicePlugins.
   */
  public void dispatchStop() {
    dispatchCall(
      new SingleArgumentRunnable<T>() {
        public void run(T p) {
          try {
            p.stop();
          } catch (Throwable t) {
            LOG.warn("ServicePlugin " + p + " could not be stopped", t);
          }
        }
      });
  }
}
