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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

abstract class AbstractAppHandle implements SparkAppHandle {

  private static final Logger LOG = Logger.getLogger(AbstractAppHandle.class.getName());

  private final LauncherServer server;

  private LauncherServer.ServerConnection connection;
  private List<SparkAppHandle.Listener> listeners;
  private AtomicReference<SparkAppHandle.State> state;
  private volatile String appId;
  private volatile boolean disposed;

  protected AbstractAppHandle(LauncherServer server) {
    this.server = server;
    this.state = new AtomicReference<>(State.UNKNOWN);
  }

  @Override
  public synchronized void addListener(SparkAppHandle.Listener l) {
    if (listeners == null) {
      listeners = new CopyOnWriteArrayList<>();
    }
    listeners.add(l);
  }

  @Override
  public SparkAppHandle.State getState() {
    return state.get();
  }

  @Override
  public String getAppId() {
    return appId;
  }

  @Override
  public void stop() {
    CommandBuilderUtils.checkState(connection != null, "Application is still not connected.");
    try {
      connection.send(new LauncherProtocol.Stop());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public synchronized void disconnect() {
    if (connection != null && connection.isOpen()) {
      try {
        connection.close();
      } catch (IOException ioe) {
        // no-op.
      }
    }
    dispose();
  }

  void setConnection(LauncherServer.ServerConnection connection) {
    this.connection = connection;
  }

  LauncherConnection getConnection() {
    return connection;
  }

  boolean isDisposed() {
    return disposed;
  }

  /**
   * Mark the handle as disposed, and set it as LOST in case the current state is not final.
   *
   * This method should be called only when there's a reasonable expectation that the communication
   * with the child application is not needed anymore, either because the code managing the handle
   * has said so, or because the child application is finished.
   */
  synchronized void dispose() {
    if (!isDisposed()) {
      // First wait for all data from the connection to be read. Then unregister the handle.
      // Otherwise, unregistering might cause the server to be stopped and all child connections
      // to be closed.
      if (connection != null) {
        try {
          connection.waitForClose();
        } catch (IOException ioe) {
          // no-op.
        }
      }
      server.unregister(this);

      // Set state to LOST if not yet final.
      setState(State.LOST, false);
      this.disposed = true;
    }
  }

  void setState(SparkAppHandle.State s) {
    setState(s, false);
  }

  void setState(SparkAppHandle.State s, boolean force) {
    if (force) {
      state.set(s);
      fireEvent(false);
      return;
    }

    State current = state.get();
    while (!current.isFinal()) {
      if (state.compareAndSet(current, s)) {
        fireEvent(false);
        return;
      }
      current = state.get();
    }

    if (s != State.LOST) {
      LOG.log(Level.WARNING, "Backend requested transition from final state {0} to {1}.",
        new Object[] { current, s });
    }
  }

  void setAppId(String appId) {
    this.appId = appId;
    fireEvent(true);
  }

  private void fireEvent(boolean isInfoChanged) {
    if (listeners != null) {
      for (Listener l : listeners) {
        if (isInfoChanged) {
          l.infoChanged(this);
        } else {
          l.stateChanged(this);
        }
      }
    }
  }

}
