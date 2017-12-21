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

package org.apache.spark.launcher;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

class InProcessAppHandle extends AbstractAppHandle {

  private static final String THREAD_NAME_FMT = "spark-app-%d: '%s'";
  private static final Logger LOG = Logger.getLogger(ChildProcAppHandle.class.getName());
  private static final AtomicLong THREAD_IDS = new AtomicLong();

  // Avoid really long thread names.
  private static final int MAX_APP_NAME_LEN = 16;

  private Thread app;

  InProcessAppHandle(LauncherServer server) {
    super(server);
  }

  @Override
  public synchronized void kill() {
    LOG.warning("kill() may leave the underlying app running in in-process mode.");
    disconnect();

    // Interrupt the thread. This is not guaranteed to kill the app, though.
    if (app != null) {
      app.interrupt();
    }

    setState(State.KILLED);
  }

  synchronized void start(String appName, Method main, String[] args) {
    CommandBuilderUtils.checkState(app == null, "Handle already started.");

    if (appName.length() > MAX_APP_NAME_LEN) {
      appName = "..." + appName.substring(appName.length() - MAX_APP_NAME_LEN);
    }

    app = new Thread(() -> {
      try {
        main.invoke(null, (Object) args);
      } catch (Throwable t) {
        LOG.log(Level.WARNING, "Application failed with exception.", t);
        setState(State.FAILED);
      }

      synchronized (InProcessAppHandle.this) {
        if (!isDisposed()) {
          disconnect();
          if (!getState().isFinal()) {
            setState(State.LOST, true);
          }
        }
      }
    });

    app.setName(String.format(THREAD_NAME_FMT, THREAD_IDS.incrementAndGet(), appName));
    app.start();
  }

}
