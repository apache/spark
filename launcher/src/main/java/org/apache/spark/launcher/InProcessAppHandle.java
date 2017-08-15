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

import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

class InProcessAppHandle extends AbstractAppHandle {

  private static final Logger LOG = Logger.getLogger(ChildProcAppHandle.class.getName());
  private static final ThreadFactory THREAD_FACTORY =
    new NamedThreadFactory("spark-app-monitor-%d");

  private Thread thread;

  InProcessAppHandle(LauncherServer server) {
    super(server);
  }

  @Override
  public synchronized void kill() {
    LOG.warning("kill() may leave the underlying app running in in-process mode.");
    disconnect();

    // Interrupt the thread. This is not guaranteed to kill the app, though.
    if (thread != null) {
      thread.interrupt();
    }

    setState(State.KILLED);
  }

  synchronized void setAppThread(Thread thread) {
    this.thread = thread;
    THREAD_FACTORY.newThread(this::monitorChild).start();
  }

  private void monitorChild() {
    try {
      while (thread.isAlive() && !isDisposed()) {
        thread.join();
      }
    } catch (InterruptedException ie) {
      // Ignore.
    }

    synchronized (this) {
      if (isDisposed()) {
        return;
      }

      State currState = getState();
      disconnect();

      if (!currState.isFinal()) {
        setState(State.LOST, true);
      }
    }
  }

}
