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

package org.apache.hadoop.eclipse;

import org.apache.hadoop.eclipse.servers.ServerRegistry;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.osgi.framework.BundleContext;

/**
 * The activator class controls the plug-in life cycle
 */
public class Activator extends AbstractUIPlugin {

  /**
   * The plug-in ID
   */
  public static final String PLUGIN_ID = "org.apache.hadoop.eclipse";

  /**
   * The shared unique instance (singleton)
   */
  private static Activator plugin;

  /**
   * Constructor
   */
  public Activator() {
    synchronized (Activator.class) {
      if (plugin != null) {
        // Not a singleton!?
        throw new RuntimeException("Activator for " + PLUGIN_ID
            + " is not a singleton");
      }
      plugin = this;
    }
  }

  /* @inheritDoc */
  @Override
  public void start(BundleContext context) throws Exception {
    super.start(context);
  }

  /* @inheritDoc */
  @Override
  public void stop(BundleContext context) throws Exception {
    ServerRegistry.getInstance().dispose();
    plugin = null;
    super.stop(context);
  }

  /**
   * Returns the shared unique instance (singleton)
   * 
   * @return the shared unique instance (singleton)
   */
  public static Activator getDefault() {
    return plugin;
  }

}
