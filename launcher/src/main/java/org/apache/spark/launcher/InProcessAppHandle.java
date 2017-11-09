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
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

class InProcessAppHandle extends AbstractAppHandle {

  private static final Logger LOG = Logger.getLogger(ChildProcAppHandle.class.getName());
  private static final ThreadFactory THREAD_FACTORY = new NamedThreadFactory("spark-app-%d");

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

  synchronized void start(Method main, String[] args) {
    CommandBuilderUtils.checkState(app == null, "Handle already started.");
    app = THREAD_FACTORY.newThread(() -> {
      try {
        main.invoke(null, (Object) args);
      } catch (Throwable t) {
        LOG.log(Level.WARNING, "Application failed with exception.", t);
        setState(State.FAILED);
      }

      synchronized (InProcessAppHandle.this) {
        if (!isDisposed()) {
          State currState = getState();
          disconnect();

          if (!currState.isFinal()) {
            setState(State.LOST, true);
          }
        }
      }
    });
    app.start();
  }

}
